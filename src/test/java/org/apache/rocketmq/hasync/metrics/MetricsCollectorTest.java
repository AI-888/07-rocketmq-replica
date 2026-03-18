package org.apache.rocketmq.hasync.metrics;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * MetricsCollector 单元测试
 */
public class MetricsCollectorTest {

    private MetricsCollector collector;

    @Before
    public void setUp() {
        collector = new MetricsCollector();
    }

    // ==================== 初始状态 ====================

    @Test
    public void testInitialSourceMetrics() {
        assertEquals("DISCONNECTED", collector.getConnectionStatus());
        assertEquals("", collector.getCurrentMasterAddr());
        assertEquals(0L, collector.getContinuousFailDurationSeconds());
        assertEquals(0L, collector.getConnectionErrorCount());
        assertEquals(0L, collector.getRetryCount());
        assertEquals(0L, collector.getNameSrvQueryErrorCount());
        assertEquals(0L, collector.getParseErrorCount());
        assertEquals(0L, collector.getHalfPacketDropCount());
        assertEquals(0L, collector.getOffsetMismatchCount());
        assertEquals(0L, collector.getMasterSwitchCount());
        assertEquals("RUNNING", collector.getParseErrorSuspendStatus());
        assertEquals(0L, collector.getParseErrorSuspendDurationSeconds());
        assertEquals(0L, collector.getParseErrorSuspendCount());
    }

    @Test
    public void testInitialSinkMetrics() {
        assertEquals(0L, collector.getSyncSuccessCount());
        assertEquals(0L, collector.getSyncFailureCount());
        assertEquals(0L, collector.getFilteredMessageCount());
        assertEquals(0L, collector.getStorageWriteErrorCount());
        assertEquals(0L, collector.getCheckpointFlushErrorCount());
        assertEquals("SKIPPED", collector.getStartupCheckResult());
        assertEquals(0L, collector.getStartupCheckMsgFound());
        assertEquals("AVAILABLE", collector.getTargetClusterStatus());
        assertEquals(0L, collector.getTargetUnavailableDurationSeconds());
        assertEquals(0L, collector.getTargetProbeSuccessCount());
        assertEquals(0L, collector.getTargetProbeFailureCount());
        assertEquals(0L, collector.getRfqSendSuccessCount());
        assertEquals(0L, collector.getRfqSendFailureCount());
        assertEquals(0L, collector.getRfqFallbackCount());
        assertEquals(0L, collector.getTopicSyncOnDemandCount());
        assertEquals(0L, collector.getTopicSyncFailureCount());
        assertFalse(collector.isTopicSyncSuspended());
        assertEquals("", collector.getTopicSyncFailedTopic());
    }

    @Test
    public void testInitialPipelineMetrics() {
        assertEquals(0L, collector.getSyncBytesPerSecond());
        assertEquals(0, collector.getQueueSize());
        assertEquals(0L, collector.getConfirmedOffset());
        assertEquals(0L, collector.getMasterOffset());
        assertEquals(0L, collector.getLagBytes());
        assertEquals("", collector.getLastCheckpointFlushTime());
        assertEquals(0L, collector.getMetaSyncSuccessCount());
        assertEquals(0L, collector.getMetaSyncErrorCount());
        assertEquals("", collector.getLastMetaSyncTime());
        assertEquals(0L, collector.getAvgEndToEndLatencyMs());
        assertEquals(0L, collector.getP99EndToEndLatencyMs());
        assertEquals(0L, collector.getCurrentTps());
    }

    // ==================== Source 侧 increment ====================

    @Test
    public void testSourceIncrements() {
        collector.incrementConnectionErrorCount();
        collector.incrementConnectionErrorCount();
        assertEquals(2L, collector.getConnectionErrorCount());

        collector.incrementRetryCount();
        assertEquals(1L, collector.getRetryCount());

        collector.incrementNameSrvQueryErrorCount();
        collector.incrementNameSrvQueryErrorCount();
        collector.incrementNameSrvQueryErrorCount();
        assertEquals(3L, collector.getNameSrvQueryErrorCount());

        for (int i = 0; i < 10; i++) {
            collector.incrementParseErrorCount();
        }
        assertEquals(10L, collector.getParseErrorCount());

        collector.incrementHalfPacketDropCount();
        assertEquals(1L, collector.getHalfPacketDropCount());

        collector.incrementOffsetMismatchCount();
        assertEquals(1L, collector.getOffsetMismatchCount());

        collector.incrementMasterSwitchCount();
        collector.incrementMasterSwitchCount();
        assertEquals(2L, collector.getMasterSwitchCount());

        collector.incrementParseErrorSuspendCount();
        assertEquals(1L, collector.getParseErrorSuspendCount());
    }

    // ==================== Sink 侧 increment ====================

    @Test
    public void testSinkIncrements() {
        for (int i = 0; i < 100; i++) {
            collector.incrementSyncSuccessCount();
        }
        assertEquals(100L, collector.getSyncSuccessCount());

        collector.incrementSyncFailureCount();
        assertEquals(1L, collector.getSyncFailureCount());

        collector.incrementFilteredMessageCount();
        collector.incrementFilteredMessageCount();
        assertEquals(2L, collector.getFilteredMessageCount());

        collector.incrementStorageWriteErrorCount();
        assertEquals(1L, collector.getStorageWriteErrorCount());

        collector.incrementCheckpointFlushErrorCount();
        assertEquals(1L, collector.getCheckpointFlushErrorCount());

        collector.incrementTargetProbeSuccessCount();
        assertEquals(1L, collector.getTargetProbeSuccessCount());

        collector.incrementTargetProbeFailureCount();
        assertEquals(1L, collector.getTargetProbeFailureCount());

        collector.incrementRfqSendSuccessCount();
        assertEquals(1L, collector.getRfqSendSuccessCount());

        collector.incrementRfqSendFailureCount();
        assertEquals(1L, collector.getRfqSendFailureCount());

        collector.incrementRfqFallbackCount();
        assertEquals(1L, collector.getRfqFallbackCount());

        collector.incrementTopicSyncOnDemandCount();
        assertEquals(1L, collector.getTopicSyncOnDemandCount());

        collector.incrementTopicSyncFailureCount();
        assertEquals(1L, collector.getTopicSyncFailureCount());
    }

    // ==================== Pipeline 侧 increment ====================

    @Test
    public void testPipelineIncrements() {
        collector.incrementMetaSyncSuccessCount();
        collector.incrementMetaSyncSuccessCount();
        assertEquals(2L, collector.getMetaSyncSuccessCount());

        collector.incrementMetaSyncErrorCount();
        assertEquals(1L, collector.getMetaSyncErrorCount());
    }

    // ==================== Setters ====================

    @Test
    public void testSourceSetters() {
        collector.setConnectionStatus("CONNECTED");
        assertEquals("CONNECTED", collector.getConnectionStatus());

        collector.setCurrentMasterAddr("192.168.1.1:10912");
        assertEquals("192.168.1.1:10912", collector.getCurrentMasterAddr());

        collector.setContinuousFailDurationSeconds(120);
        assertEquals(120L, collector.getContinuousFailDurationSeconds());

        collector.setParseErrorSuspendStatus("PARSE_ERROR_SUSPENDED");
        assertEquals("PARSE_ERROR_SUSPENDED", collector.getParseErrorSuspendStatus());

        collector.setParseErrorSuspendDurationSeconds(60);
        assertEquals(60L, collector.getParseErrorSuspendDurationSeconds());
    }

    @Test
    public void testSinkSetters() {
        collector.setStartupCheckResult("PASSED");
        assertEquals("PASSED", collector.getStartupCheckResult());

        collector.setStartupCheckMsgFound(10);
        assertEquals(10L, collector.getStartupCheckMsgFound());

        collector.setTargetClusterStatus("UNAVAILABLE");
        assertEquals("UNAVAILABLE", collector.getTargetClusterStatus());

        collector.setTargetUnavailableDurationSeconds(300);
        assertEquals(300L, collector.getTargetUnavailableDurationSeconds());

        collector.setTopicSyncSuspended(true, "TestTopic");
        assertTrue(collector.isTopicSyncSuspended());
        assertEquals("TestTopic", collector.getTopicSyncFailedTopic());

        collector.setTopicSyncSuspended(false, "");
        assertFalse(collector.isTopicSyncSuspended());
        assertEquals("", collector.getTopicSyncFailedTopic());
    }

    @Test
    public void testPipelineSetters() {
        collector.setSyncBytesPerSecond(1048576);
        assertEquals(1048576L, collector.getSyncBytesPerSecond());

        collector.setQueueSize(42);
        assertEquals(42, collector.getQueueSize());

        collector.setConfirmedOffset(1234567890L);
        assertEquals(1234567890L, collector.getConfirmedOffset());

        collector.setMasterOffset(1234667890L);
        assertEquals(1234667890L, collector.getMasterOffset());

        collector.setLagBytes(100000);
        assertEquals(100000L, collector.getLagBytes());

        collector.setLastCheckpointFlushTime("2026-03-17T10:30:00Z");
        assertEquals("2026-03-17T10:30:00Z", collector.getLastCheckpointFlushTime());

        collector.setLastMetaSyncTime("2026-03-17T10:29:00Z");
        assertEquals("2026-03-17T10:29:00Z", collector.getLastMetaSyncTime());

        collector.setAvgEndToEndLatencyMs(45);
        assertEquals(45L, collector.getAvgEndToEndLatencyMs());

        collector.setP99EndToEndLatencyMs(120);
        assertEquals(120L, collector.getP99EndToEndLatencyMs());

        collector.setCurrentTps(5000);
        assertEquals(5000L, collector.getCurrentTps());
    }

    // ==================== 快照方法 ====================

    @Test
    public void testGetSourceMetrics() {
        collector.setConnectionStatus("CONNECTED");
        collector.setCurrentMasterAddr("10.0.0.1:10912");
        collector.incrementConnectionErrorCount();

        Map<String, Object> source = collector.getSourceMetrics();
        assertEquals(13, source.size());
        assertEquals("CONNECTED", source.get("connectionStatus"));
        assertEquals("10.0.0.1:10912", source.get("currentMasterAddr"));
        assertEquals(1L, source.get("connectionErrorCount"));
    }

    @Test
    public void testGetSinkMetrics() {
        collector.incrementSyncSuccessCount();
        collector.incrementSyncSuccessCount();
        collector.setTargetClusterStatus("UNAVAILABLE");

        Map<String, Object> sink = collector.getSinkMetrics();
        assertEquals(19, sink.size());
        assertEquals(2L, sink.get("syncSuccessCount"));
        assertEquals("UNAVAILABLE", sink.get("targetClusterStatus"));
    }

    @Test
    public void testGetPipelineMetrics() {
        collector.setQueueSize(100);
        collector.setConfirmedOffset(9999L);
        collector.setCurrentTps(3000);

        Map<String, Object> pipeline = collector.getPipelineMetrics();
        assertEquals(12, pipeline.size());
        assertEquals(100, pipeline.get("queueSize"));
        assertEquals(9999L, pipeline.get("confirmedOffset"));
        assertEquals(3000L, pipeline.get("currentTps"));
    }

    @Test
    public void testGetAllMetrics() {
        Map<String, Object> all = collector.getAllMetrics();
        assertEquals(3, all.size());
        assertTrue(all.containsKey("source"));
        assertTrue(all.containsKey("sink"));
        assertTrue(all.containsKey("pipeline"));

        assertTrue(all.get("source") instanceof Map);
        assertTrue(all.get("sink") instanceof Map);
        assertTrue(all.get("pipeline") instanceof Map);
    }

    // ==================== 重置 ====================

    @Test
    public void testReset() {
        // 设置一些值
        collector.setConnectionStatus("CONNECTED");
        collector.incrementSyncSuccessCount();
        collector.incrementParseErrorCount();
        collector.setQueueSize(50);
        collector.setConfirmedOffset(1000L);
        collector.setTargetClusterStatus("UNAVAILABLE");
        collector.setTopicSyncSuspended(true, "TestTopic");

        // 验证非零
        assertEquals("CONNECTED", collector.getConnectionStatus());
        assertEquals(1L, collector.getSyncSuccessCount());
        assertEquals(50, collector.getQueueSize());
        assertTrue(collector.isTopicSyncSuspended());

        // 重置
        collector.reset();

        // 验证归零
        assertEquals("DISCONNECTED", collector.getConnectionStatus());
        assertEquals(0L, collector.getSyncSuccessCount());
        assertEquals(0L, collector.getParseErrorCount());
        assertEquals(0, collector.getQueueSize());
        assertEquals(0L, collector.getConfirmedOffset());
        assertEquals("AVAILABLE", collector.getTargetClusterStatus());
        assertFalse(collector.isTopicSyncSuspended());
        assertEquals("", collector.getTopicSyncFailedTopic());
    }

    // ==================== 线程安全 ====================

    @Test
    public void testConcurrentIncrements() throws InterruptedException {
        int threads = 10;
        int incrementsPerThread = 1000;
        Thread[] workers = new Thread[threads];

        for (int i = 0; i < threads; i++) {
            workers[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    collector.incrementSyncSuccessCount();
                    collector.incrementParseErrorCount();
                    collector.incrementRetryCount();
                }
            });
            workers[i].start();
        }

        for (Thread t : workers) {
            t.join();
        }

        long expected = (long) threads * incrementsPerThread;
        assertEquals(expected, collector.getSyncSuccessCount());
        assertEquals(expected, collector.getParseErrorCount());
        assertEquals(expected, collector.getRetryCount());
    }

    @Test
    public void testConcurrentGetAllMetrics() throws InterruptedException {
        // 一边写指标一边读快照，不应抛异常
        Thread writer = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                collector.incrementSyncSuccessCount();
                collector.setConnectionStatus(i % 2 == 0 ? "CONNECTED" : "RECONNECTING");
                collector.setQueueSize(i);
            }
        });

        Thread reader = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                Map<String, Object> all = collector.getAllMetrics();
                assertNotNull(all);
                assertEquals(3, all.size());
            }
        });

        writer.start();
        reader.start();
        writer.join();
        reader.join();

        // 确保最终值合理
        assertTrue(collector.getSyncSuccessCount() > 0);
    }
}
