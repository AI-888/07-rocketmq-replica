package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Topic 按需同步 — 目标集群不存在 Topic 时自动从源集群同步创建
 * <p>
 * 对应需求 12 §9-17：
 * <ul>
 *   <li>Sink 写入前检查 Topic 是否存在于目标集群</li>
 *   <li>不存在则从源集群查询 TopicConfig 并在目标集群创建</li>
 *   <li>创建失败时指数退避重试（最多 topicSyncMaxRetry 次）</li>
 *   <li>重试均失败 → 暂停 Sink + 启动自动恢复任务（每 30 秒重试）</li>
 * </ul>
 */
public class TopicOnDemandSync {

    private static final Logger log = LoggerFactory.getLogger(TopicOnDemandSync.class);

    /** 自动恢复重试间隔 */
    private static final long AUTO_RECOVERY_INTERVAL_MS = 30_000;

    private final int maxRetry;
    private final Set<String> localTopicCache = ConcurrentHashMap.newKeySet();
    private volatile boolean suspended = false;
    private volatile String failedTopic = "";
    private MetricsCollector metricsCollector;
    private ScheduledExecutorService recoveryScheduler;

    /** Topic 同步回调 */
    private TopicSyncCallback syncCallback;

    /**
     * Topic 同步回调接口
     */
    public interface TopicSyncCallback {
        /**
         * 从源集群同步 Topic 到目标集群
         *
         * @param topic Topic 名称
         * @return true 同步成功
         * @throws Exception 同步失败
         */
        boolean syncTopic(String topic) throws Exception;
    }

    public TopicOnDemandSync(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    public void setSyncCallback(TopicSyncCallback callback) {
        this.syncCallback = callback;
    }

    public void setMetricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    /**
     * 初始化本地 Topic 缓存
     */
    public void initCache(Set<String> existingTopics) {
        if (existingTopics != null) {
            localTopicCache.addAll(existingTopics);
            log.info("本地 Topic 缓存已初始化，共 {} 个 Topic", localTopicCache.size());
        }
    }

    /**
     * 确保 Topic 在目标集群中存在
     *
     * @param topic Topic 名称
     * @return true 表示 Topic 已就绪，false 表示同步失败（Sink 应暂停）
     */
    public boolean ensureTopicExists(String topic) {
        // 1. 检查本地缓存
        if (localTopicCache.contains(topic)) {
            return true;
        }

        // 2. 已暂停状态
        if (suspended) {
            log.warn("Topic 同步已暂停（失败 Topic: {}），消息 Topic: {} 被阻塞", failedTopic, topic);
            return false;
        }

        if (syncCallback == null) {
            log.warn("TopicSyncCallback 未设置，无法同步 Topic: {}", topic);
            return false;
        }

        if (metricsCollector != null) {
            metricsCollector.incrementTopicSyncOnDemandCount();
        }

        // 3. 尝试同步
        log.info("Topic [{}] 在目标集群不存在，开始按需同步", topic);
        return retryTopicSync(topic);
    }

    /**
     * 指数退避重试
     */
    private boolean retryTopicSync(String topic) {
        long retryInterval = 500;

        for (int i = 0; i <= maxRetry; i++) {
            try {
                if (i > 0) {
                    Thread.sleep(retryInterval);
                    retryInterval = Math.min(retryInterval * 2, 5000);
                }

                if (syncCallback.syncTopic(topic)) {
                    localTopicCache.add(topic);
                    log.info("Topic [{}] 已从源集群同步创建成功", topic);
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                log.warn("Topic 同步重试 {}/{} 失败: topic={}, error={}",
                        i + 1, maxRetry + 1, topic, e.getMessage());
            }
        }

        // 重试均失败 → 暂停 Sink
        suspended = true;
        failedTopic = topic;

        if (metricsCollector != null) {
            metricsCollector.incrementTopicSyncFailureCount();
            metricsCollector.setTopicSyncSuspended(true, topic);
        }

        log.error("Topic [{}] 同步到目标集群失败（已重试 {} 次），Sink 已暂停", topic, maxRetry + 1);

        // 启动自动恢复任务
        startAutoRecovery(topic);

        return false;
    }

    /**
     * 启动自动恢复任务（每 30 秒重试一次）
     */
    private void startAutoRecovery(String topic) {
        if (recoveryScheduler != null) {
            recoveryScheduler.shutdown();
        }

        recoveryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "topic-sync-recovery");
            t.setDaemon(true);
            return t;
        });

        recoveryScheduler.scheduleAtFixedRate(() -> {
            try {
                if (syncCallback != null && syncCallback.syncTopic(topic)) {
                    localTopicCache.add(topic);
                    resume();
                    recoveryScheduler.shutdown();
                    log.info("Topic [{}] 自动恢复成功", topic);
                }
            } catch (Exception e) {
                log.debug("Topic [{}] 自动恢复重试失败: {}", topic, e.getMessage());
            }
        }, AUTO_RECOVERY_INTERVAL_MS, AUTO_RECOVERY_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 手动恢复暂停状态
     */
    public void resume() {
        if (suspended) {
            suspended = false;
            failedTopic = "";
            if (metricsCollector != null) {
                metricsCollector.setTopicSyncSuspended(false, "");
            }
            log.info("Topic 同步已从暂停状态恢复");
        }
    }

    /**
     * 停止
     */
    public void stop() {
        if (recoveryScheduler != null) {
            recoveryScheduler.shutdown();
        }
    }

    // ==================== Getters ====================

    public boolean isSuspended() {
        return suspended;
    }

    public String getFailedTopic() {
        return failedTopic;
    }

    public Set<String> getLocalTopicCache() {
        return localTopicCache;
    }

    public int getMaxRetry() {
        return maxRetry;
    }
}
