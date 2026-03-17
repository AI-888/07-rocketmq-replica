package org.apache.rocketmq.hasync.trace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 全链路 Trace 采集器
 * <p>
 * 对应需求 19 §1-6：
 * <ul>
 *   <li>Trace ID 格式：{sourceNodeId}-{masterPhyOffset}-{offsetInPacket}</li>
 *   <li>事件类型：SOURCE_PARSED / SINK_WRITTEN / FAILED</li>
 *   <li>端到端延迟计算（avg / P99，滑动窗口 1 分钟）</li>
 *   <li>TPS 计算（滑动窗口 1 秒）</li>
 * </ul>
 */
public class TraceCollector {

    private static final Logger log = LoggerFactory.getLogger(TraceCollector.class);

    /** 延迟统计滑动窗口大小（毫秒） */
    private static final long LATENCY_WINDOW_MS = 60_000;
    /** TPS 统计滑动窗口大小（毫秒） */
    private static final long TPS_WINDOW_MS = 1_000;

    /** 延迟样本队列（滑动窗口内） */
    private final ConcurrentLinkedDeque<LatencySample> latencySamples = new ConcurrentLinkedDeque<>();

    /** TPS 计数器 */
    private final ConcurrentLinkedDeque<Long> tpsTimestamps = new ConcurrentLinkedDeque<>();

    /** 累计 Trace 事件计数 */
    private final AtomicLong sourceParsedCount = new AtomicLong(0);
    private final AtomicLong sinkWrittenCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);

    /**
     * 延迟样本
     */
    private static class LatencySample {
        final long timestamp;
        final long latencyMs;

        LatencySample(long timestamp, long latencyMs) {
            this.timestamp = timestamp;
            this.latencyMs = latencyMs;
        }
    }

    /**
     * Trace 事件
     */
    public static class TraceEvent {
        private final String traceId;
        private final String eventType;    // SOURCE_PARSED / SINK_WRITTEN / FAILED
        private final String stage;        // SOURCE / SINK
        private final long timestamp;
        private final Map<String, String> attributes;

        public TraceEvent(String traceId, String eventType, String stage, long timestamp) {
            this.traceId = traceId;
            this.eventType = eventType;
            this.stage = stage;
            this.timestamp = timestamp;
            this.attributes = new LinkedHashMap<>();
        }

        public TraceEvent attr(String key, String value) {
            this.attributes.put(key, value);
            return this;
        }

        public String getTraceId() { return traceId; }
        public String getEventType() { return eventType; }
        public String getStage() { return stage; }
        public long getTimestamp() { return timestamp; }
        public Map<String, String> getAttributes() { return attributes; }

        @Override
        public String toString() {
            return "TraceEvent{traceId='" + traceId + "', type='" + eventType
                    + "', stage='" + stage + "', ts=" + timestamp + ", attrs=" + attributes + '}';
        }
    }

    /**
     * 生成 Trace ID
     *
     * @param sourceNodeId     Source 节点 ID
     * @param masterPhyOffset  数据包起始物理偏移量
     * @param offsetInPacket   包内偏移量
     * @return Trace ID 字符串
     */
    public static String generateTraceId(String sourceNodeId, long masterPhyOffset, long offsetInPacket) {
        return sourceNodeId + "-" + masterPhyOffset + "-" + offsetInPacket;
    }

    /**
     * 记录 SOURCE_PARSED 事件
     */
    public void logSourceParsed(String traceId, long masterPhyOffset,
                                String topic, int msgSize, long parseTimestamp) {
        sourceParsedCount.incrementAndGet();

        if (log.isTraceEnabled()) {
            log.trace("Trace SOURCE_PARSED: traceId={}, offset={}, topic={}, size={}, ts={}",
                    traceId, masterPhyOffset, topic, msgSize, parseTimestamp);
        }
    }

    /**
     * 记录 SINK_WRITTEN 事件
     *
     * @param latencyMs 端到端延迟（从 Source 接收到 Sink 写入的时间差）
     */
    public void logSinkWritten(String traceId, String targetMsgId,
                               long writeTimestamp, long latencyMs) {
        sinkWrittenCount.incrementAndGet();

        // 记录延迟样本
        latencySamples.addLast(new LatencySample(writeTimestamp, latencyMs));

        // 记录 TPS 时间戳
        tpsTimestamps.addLast(writeTimestamp);

        // 清理过期样本
        cleanExpiredSamples();

        if (log.isTraceEnabled()) {
            log.trace("Trace SINK_WRITTEN: traceId={}, msgId={}, ts={}, latency={}ms",
                    traceId, targetMsgId, writeTimestamp, latencyMs);
        }
    }

    /**
     * 记录 FAILED 事件
     */
    public void logFailed(String traceId, String failStage, String errorReason, long failTimestamp) {
        failedCount.incrementAndGet();

        log.warn("Trace FAILED: traceId={}, stage={}, reason={}, ts={}",
                traceId, failStage, errorReason, failTimestamp);
    }

    /**
     * 计算平均端到端延迟（滑动窗口 1 分钟）
     */
    public long computeAvgLatencyMs() {
        cleanExpiredSamples();

        long now = System.currentTimeMillis();
        long sum = 0;
        int count = 0;

        for (LatencySample sample : latencySamples) {
            if (now - sample.timestamp <= LATENCY_WINDOW_MS) {
                sum += sample.latencyMs;
                count++;
            }
        }

        return count > 0 ? sum / count : 0;
    }

    /**
     * 计算 P99 端到端延迟（滑动窗口 1 分钟）
     */
    public long computeP99LatencyMs() {
        cleanExpiredSamples();

        long now = System.currentTimeMillis();
        List<Long> latencies = new ArrayList<>();

        for (LatencySample sample : latencySamples) {
            if (now - sample.timestamp <= LATENCY_WINDOW_MS) {
                latencies.add(sample.latencyMs);
            }
        }

        if (latencies.isEmpty()) {
            return 0;
        }

        Collections.sort(latencies);
        int p99Index = (int) Math.ceil(latencies.size() * 0.99) - 1;
        p99Index = Math.max(0, Math.min(p99Index, latencies.size() - 1));
        return latencies.get(p99Index);
    }

    /**
     * 计算当前 TPS（滑动窗口 1 秒）
     */
    public long computeCurrentTps() {
        long now = System.currentTimeMillis();
        long cutoff = now - TPS_WINDOW_MS;

        // 清理过期
        while (!tpsTimestamps.isEmpty() && tpsTimestamps.peekFirst() < cutoff) {
            tpsTimestamps.pollFirst();
        }

        return tpsTimestamps.size();
    }

    /**
     * 清理过期样本
     */
    private void cleanExpiredSamples() {
        long now = System.currentTimeMillis();
        long latencyCutoff = now - LATENCY_WINDOW_MS;
        long tpsCutoff = now - TPS_WINDOW_MS * 60; // TPS 保留 60 秒

        while (!latencySamples.isEmpty() && latencySamples.peekFirst().timestamp < latencyCutoff) {
            latencySamples.pollFirst();
        }

        while (!tpsTimestamps.isEmpty() && tpsTimestamps.peekFirst() < tpsCutoff) {
            tpsTimestamps.pollFirst();
        }
    }

    // ==================== Getters ====================

    public long getSourceParsedCount() {
        return sourceParsedCount.get();
    }

    public long getSinkWrittenCount() {
        return sinkWrittenCount.get();
    }

    public long getFailedCount() {
        return failedCount.get();
    }

    public int getLatencySampleCount() {
        return latencySamples.size();
    }
}
