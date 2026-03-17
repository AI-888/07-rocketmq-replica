package org.apache.rocketmq.hasync.trace;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TraceCollector 单元测试
 */
public class TraceCollectorTest {

    private TraceCollector collector;

    @Before
    public void setUp() {
        collector = new TraceCollector();
    }

    @Test
    public void testGenerateTraceId() {
        String traceId = TraceCollector.generateTraceId("source-01", 1000L, 0);
        assertEquals("source-01-1000-0", traceId);
    }

    @Test
    public void testLogSourceParsed() {
        collector.logSourceParsed("trace-1", 1000L, "TestTopic", 256, System.currentTimeMillis());
        assertEquals(1, collector.getSourceParsedCount());
    }

    @Test
    public void testLogSinkWritten() {
        long now = System.currentTimeMillis();
        collector.logSinkWritten("trace-1", "msg-001", now, 50);
        assertEquals(1, collector.getSinkWrittenCount());
    }

    @Test
    public void testLogFailed() {
        collector.logFailed("trace-1", "SOURCE", "CRC_MISMATCH", System.currentTimeMillis());
        assertEquals(1, collector.getFailedCount());
    }

    @Test
    public void testAvgLatency() {
        long now = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            collector.logSinkWritten("trace-" + i, "msg-" + i, now, 100);
        }
        assertEquals(100, collector.computeAvgLatencyMs());
    }

    @Test
    public void testAvgLatencyEmpty() {
        assertEquals(0, collector.computeAvgLatencyMs());
    }

    @Test
    public void testP99Latency() {
        long now = System.currentTimeMillis();
        // 99 个样本延迟 100ms，1 个样本延迟 1000ms
        for (int i = 0; i < 99; i++) {
            collector.logSinkWritten("trace-" + i, "msg-" + i, now, 100);
        }
        collector.logSinkWritten("trace-99", "msg-99", now, 1000);

        long p99 = collector.computeP99LatencyMs();
        assertTrue(p99 >= 100); // P99 至少是 100
    }

    @Test
    public void testP99LatencyEmpty() {
        assertEquals(0, collector.computeP99LatencyMs());
    }

    @Test
    public void testCurrentTps() {
        long now = System.currentTimeMillis();
        for (int i = 0; i < 50; i++) {
            collector.logSinkWritten("trace-" + i, "msg-" + i, now, 10);
        }
        long tps = collector.computeCurrentTps();
        assertTrue(tps > 0);
    }

    @Test
    public void testCurrentTpsNoData() {
        assertEquals(0, collector.computeCurrentTps());
    }

    @Test
    public void testMultipleEvents() {
        long now = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            collector.logSourceParsed("t-" + i, 1000 + i, "Topic", 100, now);
            collector.logSinkWritten("t-" + i, "msg-" + i, now, 50);
        }
        collector.logFailed("t-err", "SINK", "WRITE_FAIL", now);

        assertEquals(10, collector.getSourceParsedCount());
        assertEquals(10, collector.getSinkWrittenCount());
        assertEquals(1, collector.getFailedCount());
    }

    @Test
    public void testTraceEvent() {
        TraceCollector.TraceEvent event = new TraceCollector.TraceEvent(
                "trace-1", "SOURCE_PARSED", "SOURCE", System.currentTimeMillis());
        event.attr("topic", "TestTopic").attr("size", "256");

        assertEquals("trace-1", event.getTraceId());
        assertEquals("SOURCE_PARSED", event.getEventType());
        assertEquals("SOURCE", event.getStage());
        assertEquals(2, event.getAttributes().size());
        assertTrue(event.toString().contains("trace-1"));
    }

    @Test
    public void testLatencySampleCount() {
        long now = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            collector.logSinkWritten("t-" + i, "m-" + i, now, 10);
        }
        assertEquals(5, collector.getLatencySampleCount());
    }

    @Test
    public void testVariedLatencies() {
        long now = System.currentTimeMillis();
        collector.logSinkWritten("t1", "m1", now, 10);
        collector.logSinkWritten("t2", "m2", now, 20);
        collector.logSinkWritten("t3", "m3", now, 30);
        collector.logSinkWritten("t4", "m4", now, 40);
        collector.logSinkWritten("t5", "m5", now, 50);

        long avg = collector.computeAvgLatencyMs();
        assertEquals(30, avg); // (10+20+30+40+50) / 5 = 30
    }
}
