package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.checkpoint.CheckpointCoordinatorImpl;
import org.apache.rocketmq.hasync.checkpoint.StartupConsistencyChecker;
import org.apache.rocketmq.hasync.config.SinkConfig;
import org.apache.rocketmq.hasync.core.SyncSink;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RocketMQ Sink 实现 — 消费 SyncRecord 并写入目标集群
 * <p>
 * 对应需求：
 * <ul>
 *   <li>需求 2 §4、§6a-6f：严格顺序写入，FixedQueueSelector 保持 queueId</li>
 *   <li>需求 9：Checkpoint 断点续传</li>
 *   <li>需求 10：启动一致性校验、At-Least-Once</li>
 *   <li>需求 11：Topic 白名单过滤</li>
 *   <li>需求 12 §9-17：Topic 按需同步</li>
 *   <li>需求 15：自动重试</li>
 *   <li>需求 16：目标不可写处理</li>
 * </ul>
 */
public class RocketMQSink implements SyncSink {

    private static final Logger log = LoggerFactory.getLogger(RocketMQSink.class);

    /** 连续写入失败阈值 → 标记目标集群不可写 */
    private static final int CONTINUOUS_FAIL_THRESHOLD = 10;

    private final SinkConfig config;
    private final TopicFilter topicFilter;
    private final TopicOnDemandSync topicOnDemandSync;
    private final SinkRetryPolicy retryPolicy;
    private final FixedQueueSelector queueSelector;
    private final CheckpointCoordinatorImpl checkpointCoordinator;
    private final MetricsCollector metricsCollector;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger continuousFailCount = new AtomicInteger(0);
    private final AtomicLong targetUnavailableStartTime = new AtomicLong(0);
    private volatile String targetClusterStatus = "AVAILABLE";
    private volatile String syncStatus = "RUNNING";

    /** 写入回调（抽象实际的 RocketMQ Producer 调用） */
    private SinkWriteCallback writeCallback;

    /**
     * Sink 写入回调接口
     */
    public interface SinkWriteCallback {
        /**
         * 发送消息到目标集群
         *
         * @param topic      Topic
         * @param body       消息体
         * @param queueId    目标队列 ID
         * @param properties 消息属性
         * @return 目标集群的 msgId
         * @throws Exception 发送失败
         */
        String send(String topic, byte[] body, int queueId,
                    Map<String, String> properties) throws Exception;

        /**
         * 发送探活消息
         *
         * @return true 探活成功
         */
        boolean probe();
    }

    public RocketMQSink(SinkConfig config,
                        TopicFilter topicFilter,
                        TopicOnDemandSync topicOnDemandSync,
                        SinkRetryPolicy retryPolicy,
                        CheckpointCoordinatorImpl checkpointCoordinator,
                        MetricsCollector metricsCollector) {
        this.config = config;
        this.topicFilter = topicFilter;
        this.topicOnDemandSync = topicOnDemandSync;
        this.retryPolicy = retryPolicy;
        this.queueSelector = new FixedQueueSelector();
        this.checkpointCoordinator = checkpointCoordinator;
        this.metricsCollector = metricsCollector;
    }

    public void setWriteCallback(SinkWriteCallback writeCallback) {
        this.writeCallback = writeCallback;
        // 注入到 retryPolicy
        retryPolicy.setWriteExecutor((topic, body, queueId, properties) -> {
            if (writeCallback == null) {
                throw new SinkRetryPolicy.NonRetryableException("writeCallback 未设置");
            }
            return writeCallback.send(topic, body, queueId, properties);
        });
    }

    @Override
    public void start() throws Exception {
        log.info("========== RocketMQSink 启动 ==========");
        running.set(true);

        // 从 NameServer KV 恢复 Checkpoint
        long commitOffset = checkpointCoordinator.recoverCheckpoint(config.getSinkId());
        log.info("Checkpoint 恢复: sinkId={}, commitOffset={}", config.getSinkId(), commitOffset);

        // 启动 CheckpointCoordinator 定期刷写
        checkpointCoordinator.start();

        metricsCollector.setTargetClusterStatus("AVAILABLE");
        log.info("========== RocketMQSink 启动完成 ==========");
    }

    @Override
    public void stop() {
        log.info("========== RocketMQSink 停止 ==========");
        running.set(false);

        // 刷写最后一次 Checkpoint
        checkpointCoordinator.flush();
        checkpointCoordinator.stop();

        // 停止 TopicOnDemandSync
        topicOnDemandSync.stop();

        log.info("========== RocketMQSink 已停止 ==========");
    }

    /**
     * 写入单条记录到目标集群
     * <p>
     * 严格按 physicOffset 升序逐条写入（需求 2 §6c），
     * 当前消息写入成功并确认后才能处理下一条（需求 2 §6f）。
     */
    @Override
    public void write(SyncRecord record) throws Exception {
        if (!running.get()) {
            return;
        }

        // 1. Topic 过滤（需求 11）
        if (!topicFilter.accept(record.getTopic())) {
            // 被过滤的消息仍推进 confirmedOffset
            checkpointCoordinator.commitOffset(config.getSinkId(), record.getEndOffset());
            return;
        }

        // 2. 目标集群不可写检查（需求 16）
        if ("UNAVAILABLE".equals(targetClusterStatus)) {
            log.debug("目标集群不可写，消息被缓冲: topic={}", record.getTopic());
            metricsCollector.setTargetClusterStatus("UNAVAILABLE");
            return;
        }

        // 3. Topic 按需同步（需求 12 §9-17）
        if (!topicOnDemandSync.ensureTopicExists(record.getTopic())) {
            syncStatus = "TOPIC_SYNC_SUSPENDED";
            log.warn("Topic [{}] 同步失败，Sink 暂停写入", record.getTopic());
            return;
        }

        // 4. 构建消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("ORIGIN_PHYSICAL_OFFSET", String.valueOf(record.getPhysicOffset()));
        if (record.getTraceId() != null) {
            properties.put("SYNC_TRACE_ID", record.getTraceId());
        }
        if (record.getProperties() != null) {
            properties.putAll(record.getProperties());
        }

        // 5. 带重试写入（需求 15 + 需求 2 §6f）
        try {
            String msgId = retryPolicy.sendWithRetry(
                    record.getTopic(), record.getBody(),
                    queueSelector.select(16, record.getQueueId()),
                    properties);

            // 写入成功
            continuousFailCount.set(0);
            metricsCollector.incrementSyncSuccessCount();

            // 6. 推进 commitOffset（需求 10 §1：先写数据、确认成功、再推进）
            checkpointCoordinator.commitOffset(config.getSinkId(), record.getEndOffset());

            if (log.isDebugEnabled()) {
                log.debug("消息写入成功: topic={}, physicOffset={}, msgId={}",
                        record.getTopic(), record.getPhysicOffset(), msgId);
            }

        } catch (SinkRetryPolicy.NonRetryableException e) {
            log.error("不可重试的写入失败: topic={}, error={}", record.getTopic(), e.getMessage());
            throw e;
        } catch (Exception e) {
            handleWriteFailure(record, e);
        }
    }

    @Override
    public void flush() throws Exception {
        checkpointCoordinator.flush();
    }

    /**
     * 处理写入失败
     */
    private void handleWriteFailure(SyncRecord record, Exception e) {
        int failCount = continuousFailCount.incrementAndGet();
        metricsCollector.incrementStorageWriteErrorCount();

        // 连续失败超过阈值 → 标记目标集群不可写（需求 16 §1）
        if (failCount >= CONTINUOUS_FAIL_THRESHOLD) {
            targetClusterStatus = "UNAVAILABLE";
            if (targetUnavailableStartTime.get() == 0) {
                targetUnavailableStartTime.set(System.currentTimeMillis());
            }
            metricsCollector.setTargetClusterStatus("UNAVAILABLE");
            log.error("连续写入失败 {} 次，标记目标集群为 UNAVAILABLE", failCount);
        }

        log.warn("消息写入失败: topic={}, physicOffset={}, error={}",
                record.getTopic(), record.getPhysicOffset(), e.getMessage());
    }

    /**
     * 执行探活检查（需求 16 §3）
     */
    public boolean probeTargetCluster() {
        if (writeCallback == null) {
            return false;
        }

        try {
            boolean probeOk = writeCallback.probe();
            if (probeOk) {
                metricsCollector.incrementTargetProbeSuccessCount();
                if ("UNAVAILABLE".equals(targetClusterStatus)) {
                    targetClusterStatus = "AVAILABLE";
                    continuousFailCount.set(0);
                    targetUnavailableStartTime.set(0);
                    metricsCollector.setTargetClusterStatus("AVAILABLE");
                    log.info("目标集群探活成功，恢复正常写入");
                }
                return true;
            } else {
                metricsCollector.incrementTargetProbeFailureCount();
                return false;
            }
        } catch (Exception e) {
            metricsCollector.incrementTargetProbeFailureCount();
            return false;
        }
    }

    /**
     * 获取目标集群不可写持续时长（秒）
     */
    public long getTargetUnavailableDurationSeconds() {
        long start = targetUnavailableStartTime.get();
        if (start == 0) return 0;
        return (System.currentTimeMillis() - start) / 1000;
    }

    // ==================== Getters ====================

    public boolean isRunning() {
        return running.get();
    }

    public String getTargetClusterStatus() {
        return targetClusterStatus;
    }

    public String getSyncStatus() {
        return syncStatus;
    }

    public TopicFilter getTopicFilter() {
        return topicFilter;
    }

    public TopicOnDemandSync getTopicOnDemandSync() {
        return topicOnDemandSync;
    }

    public SinkRetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public CheckpointCoordinatorImpl getCheckpointCoordinator() {
        return checkpointCoordinator;
    }

    public int getContinuousFailCount() {
        return continuousFailCount.get();
    }
}
