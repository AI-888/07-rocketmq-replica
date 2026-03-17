package org.apache.rocketmq.hasync.alert;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 告警规则评估器
 * <p>
 * 对应需求 20 §6（告警规则）：
 * <ul>
 *   <li>Master 长时间不可用：continuousFailDurationSeconds > 600 → P0</li>
 *   <li>同步严重滞后：lagBytes > 100MB 持续 60s → P1</li>
 *   <li>写入异常频繁：syncFailureCount 60s 新增 > 10 → P1</li>
 *   <li>解析失败频繁：parseErrorCount 60s 新增 > 100 → P1</li>
 *   <li>Checkpoint 刷写异常：checkpointFlushErrorCount 60s 新增 > 3 → P0</li>
 *   <li>同步已暂停：PARSE_ERROR_SUSPENDED → P0（每 30s）</li>
 *   <li>目标集群不可写：UNAVAILABLE → P0</li>
 *   <li>目标长时间不可写：> 300s → P0</li>
 *   <li>队列积压告警：queueSize > 80% → WARN</li>
 * </ul>
 */
public class AlertEvaluator {

    private static final Logger log = LoggerFactory.getLogger(AlertEvaluator.class);

    /** 100MB */
    private static final long LAG_THRESHOLD_BYTES = 100 * 1024 * 1024L;
    /** 默认队列容量 */
    private static final int DEFAULT_QUEUE_CAPACITY = 1000;

    private final MetricsCollector metricsCollector;
    private final int queueCapacity;
    private ScheduledExecutorService scheduler;

    /** 上一轮指标快照（用于计算增量） */
    private long lastSyncFailureCount = 0;
    private long lastParseErrorCount = 0;
    private long lastCheckpointFlushErrorCount = 0;
    private long lagExceededStartTime = 0;

    /** 活跃告警列表 */
    private final List<Alert> activeAlerts = new ArrayList<>();

    /**
     * 告警信息
     */
    public static class Alert {
        private final String name;
        private final String level;  // P0 / P1 / WARN
        private final String message;
        private final long timestamp;

        public Alert(String name, String level, String message) {
            this.name = name;
            this.level = level;
            this.message = message;
            this.timestamp = System.currentTimeMillis();
        }

        public String getName() { return name; }
        public String getLevel() { return level; }
        public String getMessage() { return message; }
        public long getTimestamp() { return timestamp; }

        @Override
        public String toString() {
            return "[" + level + "] " + name + ": " + message;
        }
    }

    public AlertEvaluator(MetricsCollector metricsCollector) {
        this(metricsCollector, DEFAULT_QUEUE_CAPACITY);
    }

    public AlertEvaluator(MetricsCollector metricsCollector, int queueCapacity) {
        this.metricsCollector = metricsCollector;
        this.queueCapacity = queueCapacity;
    }

    /**
     * 启动定期告警评估（默认每 30 秒）
     */
    public void start() {
        scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "alert-evaluator");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::evaluate, 30, 30, TimeUnit.SECONDS);
        log.info("告警评估器已启动");
    }

    /**
     * 停止
     */
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    /**
     * 执行一轮告警评估
     *
     * @return 本轮产生的告警列表
     */
    public List<Alert> evaluate() {
        List<Alert> alerts = new ArrayList<>();

        // 1. Master 长时间不可用（P0）
        if (metricsCollector.getContinuousFailDurationSeconds() > 600) {
            alerts.add(new Alert("MASTER_UNAVAILABLE", "P0",
                    "Master 连续不可用超过 10 分钟（" + metricsCollector.getContinuousFailDurationSeconds() + "s）"));
        }

        // 2. 同步严重滞后（P1）
        long lagBytes = metricsCollector.getMasterOffset() - metricsCollector.getConfirmedOffset();
        if (lagBytes > LAG_THRESHOLD_BYTES) {
            if (lagExceededStartTime == 0) {
                lagExceededStartTime = System.currentTimeMillis();
            }
            long lagDuration = (System.currentTimeMillis() - lagExceededStartTime) / 1000;
            if (lagDuration >= 60) {
                alerts.add(new Alert("SYNC_LAG_HIGH", "P1",
                        "同步滞后 " + (lagBytes / 1024 / 1024) + "MB，持续 " + lagDuration + "s"));
            }
        } else {
            lagExceededStartTime = 0;
        }

        // 3. 写入异常频繁（P1）
        long currentSyncFailure = metricsCollector.getSyncFailureCount();
        long syncFailureDelta = currentSyncFailure - lastSyncFailureCount;
        lastSyncFailureCount = currentSyncFailure;
        if (syncFailureDelta > 10) {
            alerts.add(new Alert("WRITE_ERROR_FREQUENT", "P1",
                    "60s 内写入失败 " + syncFailureDelta + " 次"));
        }

        // 4. 解析失败频繁（P1）
        long currentParseError = metricsCollector.getParseErrorCount();
        long parseErrorDelta = currentParseError - lastParseErrorCount;
        lastParseErrorCount = currentParseError;
        if (parseErrorDelta > 100) {
            alerts.add(new Alert("PARSE_ERROR_FREQUENT", "P1",
                    "60s 内解析失败 " + parseErrorDelta + " 次"));
        }

        // 5. Checkpoint 刷写异常（P0）
        long currentFlushError = metricsCollector.getCheckpointFlushErrorCount();
        long flushErrorDelta = currentFlushError - lastCheckpointFlushErrorCount;
        lastCheckpointFlushErrorCount = currentFlushError;
        if (flushErrorDelta > 3) {
            alerts.add(new Alert("CHECKPOINT_FLUSH_ERROR", "P0",
                    "60s 内 Checkpoint 刷写失败 " + flushErrorDelta + " 次"));
        }

        // 6. 同步已暂停（P0）
        if ("PARSE_ERROR_SUSPENDED".equals(metricsCollector.getParseErrorSuspendStatus())) {
            alerts.add(new Alert("SYNC_SUSPENDED", "P0",
                    "数据同步已暂停（PARSE_ERROR_SUSPENDED），持续 "
                            + metricsCollector.getParseErrorSuspendDurationSeconds() + "s"));
        }

        // 7. 目标集群不可写（P0）
        if ("UNAVAILABLE".equals(metricsCollector.getTargetClusterStatus())) {
            alerts.add(new Alert("TARGET_UNAVAILABLE", "P0",
                    "目标集群不可写"));
        }

        // 8. 目标长时间不可写（P0）
        if (metricsCollector.getTargetUnavailableDurationSeconds() > 300) {
            alerts.add(new Alert("TARGET_LONG_UNAVAILABLE", "P0",
                    "目标集群不可写超过 300s（" + metricsCollector.getTargetUnavailableDurationSeconds() + "s）"));
        }

        // 9. 队列积压告警（WARN）
        if (metricsCollector.getQueueSize() > queueCapacity * 0.8) {
            alerts.add(new Alert("QUEUE_BACKLOG", "WARN",
                    "队列积压 " + metricsCollector.getQueueSize() + "/" + queueCapacity
                            + "（超过 80%），建议增加 Sink 线程数或节点数"));
        }

        // 输出告警日志
        for (Alert alert : alerts) {
            if ("P0".equals(alert.level)) {
                log.error("告警 {}", alert);
            } else if ("P1".equals(alert.level)) {
                log.warn("告警 {}", alert);
            } else {
                log.warn("告警 {}", alert);
            }
        }

        activeAlerts.clear();
        activeAlerts.addAll(alerts);

        return alerts;
    }

    /**
     * 获取当前活跃告警
     */
    public List<Alert> getActiveAlerts() {
        return Collections.unmodifiableList(activeAlerts);
    }
}
