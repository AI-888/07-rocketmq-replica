package org.apache.rocketmq.hasync.alert;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 指标日志定期输出
 * <p>
 * 对应需求 20 §4：
 * 每隔 10 秒将全部指标打印到日志（INFO 级别），
 * 格式为结构化 JSON，字段按 Source / Sink / Pipeline 三组排列。
 */
public class MetricsLogPrinter {

    private static final Logger log = LoggerFactory.getLogger(MetricsLogPrinter.class);

    /** 默认输出间隔（毫秒） */
    private static final long DEFAULT_INTERVAL_MS = 10_000;

    private final MetricsCollector metricsCollector;
    private final long intervalMs;
    private ScheduledExecutorService scheduler;

    public MetricsLogPrinter(MetricsCollector metricsCollector) {
        this(metricsCollector, DEFAULT_INTERVAL_MS);
    }

    public MetricsLogPrinter(MetricsCollector metricsCollector, long intervalMs) {
        this.metricsCollector = metricsCollector;
        this.intervalMs = intervalMs;
    }

    /**
     * 启动定期日志输出
     */
    public void start() {
        scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "metrics-log-printer");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::printMetrics, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("指标日志输出已启动: 间隔={}ms", intervalMs);
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
     * 输出当前所有指标
     */
    public void printMetrics() {
        try {
            Map<String, Object> allMetrics = metricsCollector.getAllMetrics();
            String json = JSON.toJSONString(allMetrics, JSONWriter.Feature.PrettyFormat);
            log.info("========== Metrics Snapshot ==========\n{}", json);
        } catch (Exception e) {
            log.warn("指标输出失败: {}", e.getMessage());
        }
    }

    /**
     * 获取指标的紧凑 JSON 字符串（用于 HTTP 响应）
     */
    public String getMetricsJson() {
        Map<String, Object> allMetrics = metricsCollector.getAllMetrics();
        return JSON.toJSONString(allMetrics);
    }
}
