package org.apache.rocketmq.hasync.reliability;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 元数据同步服务
 * <p>
 * 对应需求 12：
 * <ul>
 *   <li>启动时执行全量元数据同步</li>
 *   <li>运行期间定期增量同步（默认 60 秒）</li>
 *   <li>支持 Topic 配置、消费者位点、订阅组配置等元数据</li>
 *   <li>通过 DataVersion 比较实现增量同步</li>
 * </ul>
 */
public class MetadataSyncService {

    private static final Logger log = LoggerFactory.getLogger(MetadataSyncService.class);

    private final long syncInterval;
    private final AtomicLong syncSuccessCount = new AtomicLong(0);
    private final AtomicLong syncErrorCount = new AtomicLong(0);
    private volatile String lastSyncTime = "";
    private final Map<String, Long> dataVersions = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;
    private MetricsCollector metricsCollector;

    /** 元数据同步回调 */
    private MetadataSyncCallback syncCallback;

    /**
     * 元数据同步回调接口
     */
    public interface MetadataSyncCallback {
        /**
         * 同步指定类型的元数据
         *
         * @param metadataType 元数据类型（如 TOPIC_CONFIG, CONSUMER_OFFSET 等）
         * @return true 同步成功
         */
        boolean syncMetadata(String metadataType) throws Exception;

        /**
         * 获取指定类型的 DataVersion
         *
         * @param metadataType 元数据类型
         * @return DataVersion 值，用于增量比较
         */
        long getDataVersion(String metadataType) throws Exception;
    }

    /** 支持的元数据类型 */
    public static final String[] METADATA_TYPES = {
            "TOPIC_CONFIG",
            "CONSUMER_OFFSET",
            "DELAY_OFFSET",
            "SUBSCRIPTION_GROUP",
            "MESSAGE_REQUEST_MODE",
            "TIMER_METRICS"  // 需求 12 §1：仅当 timerWheelEnable=true 时同步，由 callback 实现判断
    };

    public MetadataSyncService(long syncInterval) {
        this.syncInterval = syncInterval;
    }

    public void setSyncCallback(MetadataSyncCallback callback) {
        this.syncCallback = callback;
    }

    public void setMetricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    /**
     * 启动元数据同步服务
     */
    public void start() {
        // 执行首次全量同步
        syncAll();

        // 启动定期同步
        scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "metadata-sync-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::syncAll, syncInterval, syncInterval, TimeUnit.MILLISECONDS);

        log.info("元数据同步服务已启动: syncInterval={}ms", syncInterval);
    }

    /**
     * 停止服务
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
        log.info("元数据同步服务已停止");
    }

    /**
     * 执行全量/增量同步
     */
    public void syncAll() {
        if (syncCallback == null) {
            log.debug("元数据同步回调未设置，跳过");
            return;
        }

        int successCount = 0;
        int failCount = 0;

        for (String metadataType : METADATA_TYPES) {
            try {
                // 增量同步：比较 DataVersion
                long currentVersion = syncCallback.getDataVersion(metadataType);
                Long lastVersion = dataVersions.get(metadataType);

                if (lastVersion != null && lastVersion == currentVersion) {
                    log.debug("元数据 [{}] DataVersion 未变化（{}），跳过", metadataType, currentVersion);
                    continue;
                }

                // 执行同步
                boolean ok = syncCallback.syncMetadata(metadataType);
                if (ok) {
                    dataVersions.put(metadataType, currentVersion);
                    successCount++;
                    log.debug("元数据 [{}] 同步成功，DataVersion={}", metadataType, currentVersion);
                } else {
                    failCount++;
                    log.warn("元数据 [{}] 同步返回失败", metadataType);
                }

            } catch (Exception e) {
                failCount++;
                log.warn("元数据 [{}] 同步异常: {}", metadataType, e.getMessage());
            }
        }

        // 更新指标
        if (successCount > 0) {
            syncSuccessCount.addAndGet(successCount);
            lastSyncTime = Instant.now().toString();
        }
        if (failCount > 0) {
            syncErrorCount.addAndGet(failCount);
        }

        if (metricsCollector != null) {
            metricsCollector.setMetaSyncSuccessCount(syncSuccessCount.get());
            metricsCollector.setMetaSyncErrorCount(syncErrorCount.get());
            metricsCollector.setLastMetaSyncTime(lastSyncTime);
        }

        if (successCount > 0 || failCount > 0) {
            log.info("元数据同步完成: 成功={}, 失败={}, 总计成功={}, 总计失败={}",
                    successCount, failCount, syncSuccessCount.get(), syncErrorCount.get());
        }
    }

    // ==================== Getters ====================

    public long getSyncSuccessCount() {
        return syncSuccessCount.get();
    }

    public long getSyncErrorCount() {
        return syncErrorCount.get();
    }

    public String getLastSyncTime() {
        return lastSyncTime;
    }

    public Map<String, Long> getDataVersions() {
        return dataVersions;
    }
}
