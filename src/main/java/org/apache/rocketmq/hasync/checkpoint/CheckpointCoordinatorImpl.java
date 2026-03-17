package org.apache.rocketmq.hasync.checkpoint;

import org.apache.rocketmq.hasync.core.CheckpointCoordinator;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Checkpoint 协调器实现 — 管理同步位点的读取与推进
 * <p>
 * 核心原则（需求 10 §1）：先写数据 → 确认成功 → 再推进 Checkpoint
 * <p>
 * 刷写策略（需求 9 §3）：
 * <ul>
 *   <li>累计新增数据包达到 checkpointFlushBatchSize（默认 100）</li>
 *   <li>距上次刷写超过 checkpointFlushInterval（默认 1000ms）</li>
 *   <li>优雅停机时强制刷写</li>
 * </ul>
 * <p>
 * 存储位置：目标集群 NameServer KV
 * <ul>
 *   <li>Namespace: SYNC_CHECKPOINT</li>
 *   <li>Key: {brokerName}:globalCheckpoint → 所有 Sink 最小 commitOffset</li>
 *   <li>Key: {brokerName}:sink:{sinkId}:commitOffset → 各 Sink 已提交位点</li>
 * </ul>
 */
public class CheckpointCoordinatorImpl implements CheckpointCoordinator {

    private static final Logger log = LoggerFactory.getLogger(CheckpointCoordinatorImpl.class);

    /** KV 命名空间 */
    public static final String NAMESPACE = "SYNC_CHECKPOINT";

    private final String brokerName;
    private final long flushInterval;
    private final int flushBatchSize;
    private volatile long globalCheckpoint;
    private final ConcurrentHashMap<String, Long> sinkCommitOffsets = new ConcurrentHashMap<>();
    private volatile long lastFlushTime;
    private final AtomicInteger pendingPacketCount = new AtomicInteger(0);
    private final AtomicLong totalFlushCount = new AtomicLong(0);
    private final AtomicLong flushErrorCount = new AtomicLong(0);
    private ScheduledExecutorService scheduler;
    private MetricsCollector metricsCollector;

    /** KV 持久化回调（抽象实际的 NameServer 调用） */
    private CheckpointPersistCallback persistCallback;

    /**
     * Checkpoint 持久化回调接口
     */
    public interface CheckpointPersistCallback {
        void putKVConfig(String namespace, String key, String value) throws Exception;
        String getKVConfig(String namespace, String key) throws Exception;
    }

    public CheckpointCoordinatorImpl(String brokerName, long flushInterval, int flushBatchSize) {
        this.brokerName = brokerName;
        this.flushInterval = flushInterval;
        this.flushBatchSize = flushBatchSize;
        this.lastFlushTime = System.currentTimeMillis();
    }

    public void setPersistCallback(CheckpointPersistCallback callback) {
        this.persistCallback = callback;
    }

    public void setMetricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    /**
     * 启动定期刷写调度
     */
    public void start() {
        scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "checkpoint-flush-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::periodicFlush, flushInterval, flushInterval, TimeUnit.MILLISECONDS);
        log.info("Checkpoint 协调器已启动: brokerName={}, flushInterval={}ms, flushBatchSize={}",
                brokerName, flushInterval, flushBatchSize);
    }

    /**
     * 停止协调器
     */
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // 停机时强制刷写
        flush();
        log.info("Checkpoint 协调器已停止");
    }

    @Override
    public long getConfirmedOffset() {
        return computeGlobalCheckpoint();
    }

    /**
     * Sink 写入成功后调用 — 推进位点
     * <p>
     * 需求 10 §1：先写数据、确认成功、再推进位点
     */
    @Override
    public void commitOffset(String sinkId, long offset) {
        Long previous = sinkCommitOffsets.put(sinkId, offset);
        pendingPacketCount.incrementAndGet();

        if (log.isDebugEnabled()) {
            log.debug("Checkpoint 更新: sinkId={}, offset={} (prev={})", sinkId, offset, previous);
        }

        // 判断是否需要立即刷写
        if (shouldFlush()) {
            doFlush();
        }
    }

    /**
     * 启动时从 NameServer KV 恢复位点
     */
    @Override
    public long recoverCheckpoint(String sinkId) {
        if (persistCallback == null) {
            log.warn("Checkpoint 持久化回调未设置，返回 0");
            return 0L;
        }

        try {
            String key = brokerName + ":sink:" + sinkId + ":commitOffset";
            String value = persistCallback.getKVConfig(NAMESPACE, key);
            if (value != null && !value.isEmpty()) {
                long offset = Long.parseLong(value);
                sinkCommitOffsets.put(sinkId, offset);
                log.info("从 NameServer KV 恢复 Checkpoint: sinkId={}, offset={}", sinkId, offset);
                return offset;
            }
        } catch (Exception e) {
            log.error("恢复 Checkpoint 失败: sinkId={}, error={}", sinkId, e.getMessage());
        }
        return 0L;
    }

    /**
     * 优雅停机时强制同步刷写
     */
    @Override
    public void flush() {
        doFlush();
    }

    /**
     * 计算 globalCheckpoint = min(所有 Sink 的 commitOffset)
     */
    private long computeGlobalCheckpoint() {
        if (sinkCommitOffsets.isEmpty()) {
            return globalCheckpoint;
        }

        long minOffset = Long.MAX_VALUE;
        for (Long offset : sinkCommitOffsets.values()) {
            if (offset < minOffset) {
                minOffset = offset;
            }
        }

        if (minOffset != Long.MAX_VALUE) {
            globalCheckpoint = minOffset;
        }
        return globalCheckpoint;
    }

    /**
     * 获取全局 Checkpoint（所有 Sink 的最小 commitOffset）
     */
    public long getGlobalCheckpoint() {
        return computeGlobalCheckpoint();
    }

    /**
     * 定期刷写检查
     */
    private void periodicFlush() {
        if (shouldFlush()) {
            doFlush();
        }
    }

    /**
     * 刷写触发条件判断
     */
    private boolean shouldFlush() {
        return pendingPacketCount.get() >= flushBatchSize
                || System.currentTimeMillis() - lastFlushTime >= flushInterval;
    }

    /**
     * 实际刷写逻辑
     */
    private synchronized void doFlush() {
        if (persistCallback == null) {
            return;
        }

        try {
            // 批量写入所有 Sink 的 commitOffset
            for (Map.Entry<String, Long> entry : sinkCommitOffsets.entrySet()) {
                String key = brokerName + ":sink:" + entry.getKey() + ":commitOffset";
                persistCallback.putKVConfig(NAMESPACE, key, String.valueOf(entry.getValue()));
            }

            // 写入 globalCheckpoint
            long gc = computeGlobalCheckpoint();
            String gcKey = brokerName + ":globalCheckpoint";
            persistCallback.putKVConfig(NAMESPACE, gcKey, String.valueOf(gc));

            lastFlushTime = System.currentTimeMillis();
            pendingPacketCount.set(0);
            totalFlushCount.incrementAndGet();

            if (metricsCollector != null) {
                metricsCollector.setConfirmedOffset(gc);
                metricsCollector.setLastCheckpointFlushTime(Instant.now().toString());
            }

            log.debug("Checkpoint 刷写成功: globalCheckpoint={}, sinkCount={}", gc, sinkCommitOffsets.size());

        } catch (Exception e) {
            flushErrorCount.incrementAndGet();
            if (metricsCollector != null) {
                metricsCollector.incrementCheckpointFlushError();
            }
            log.error("Checkpoint 刷写失败: {}", e.getMessage(), e);
        }
    }

    // ==================== Getters ====================

    public String getBrokerName() {
        return brokerName;
    }

    public ConcurrentHashMap<String, Long> getSinkCommitOffsets() {
        return sinkCommitOffsets;
    }

    public long getTotalFlushCount() {
        return totalFlushCount.get();
    }

    public long getFlushErrorCount() {
        return flushErrorCount.get();
    }

    public long getLastFlushTime() {
        return lastFlushTime;
    }

    public int getPendingPacketCount() {
        return pendingPacketCount.get();
    }
}
