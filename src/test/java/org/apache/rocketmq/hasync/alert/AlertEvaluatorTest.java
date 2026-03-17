package org.apache.rocketmq.hasync.alert;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * AlertEvaluator 单元测试
 */
public class AlertEvaluatorTest {

    private MetricsCollector metricsCollector;
    private AlertEvaluator evaluator;

    @Before
    public void setUp() {
        metricsCollector = new MetricsCollector();
        evaluator = new AlertEvaluator(metricsCollector, 1000);
    }

    @Test
    public void testNoAlertsWhenHealthy() {
        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertTrue(alerts.isEmpty());
    }

    @Test
    public void testMasterUnavailableAlert() {
        metricsCollector.setContinuousFailDurationSeconds(601);
        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();

        assertTrue(alerts.stream().anyMatch(a -> "MASTER_UNAVAILABLE".equals(a.getName())));
        assertTrue(alerts.stream().anyMatch(a -> "P0".equals(a.getLevel())));
    }

    @Test
    public void testWriteErrorFrequentAlert() {
        // 模拟 60s 内写入失败超过 10 次
        for (int i = 0; i < 15; i++) {
            metricsCollector.incrementSyncFailureCount();
        }

        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertTrue(alerts.stream().anyMatch(a -> "WRITE_ERROR_FREQUENT".equals(a.getName())));
    }

    @Test
    public void testParseErrorFrequentAlert() {
        // 模拟解析失败超过 100 次
        for (int i = 0; i < 101; i++) {
            metricsCollector.incrementParseErrorCount();
        }

        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertTrue(alerts.stream().anyMatch(a -> "PARSE_ERROR_FREQUENT".equals(a.getName())));
    }

    @Test
    public void testCheckpointFlushErrorAlert() {
        for (int i = 0; i < 5; i++) {
            metricsCollector.incrementCheckpointFlushErrorCount();
        }

        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertTrue(alerts.stream().anyMatch(a -> "CHECKPOINT_FLUSH_ERROR".equals(a.getName())));
    }

    @Test
    public void testSyncSuspendedAlert() {
        metricsCollector.setParseErrorSuspendStatus("PARSE_ERROR_SUSPENDED");

        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertTrue(alerts.stream().anyMatch(a -> "SYNC_SUSPENDED".equals(a.getName())));
    }

    @Test
    public void testTargetUnavailableAlert() {
        metricsCollector.setTargetClusterStatus("UNAVAILABLE");

        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertTrue(alerts.stream().anyMatch(a -> "TARGET_UNAVAILABLE".equals(a.getName())));
    }

    @Test
    public void testTargetLongUnavailableAlert() {
        metricsCollector.setTargetClusterStatus("UNAVAILABLE");
        metricsCollector.setTargetUnavailableDurationSeconds(301);

        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertTrue(alerts.stream().anyMatch(a -> "TARGET_LONG_UNAVAILABLE".equals(a.getName())));
    }

    @Test
    public void testQueueBacklogAlert() {
        metricsCollector.setQueueSize(900); // 90% of 1000

        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertTrue(alerts.stream().anyMatch(a -> "QUEUE_BACKLOG".equals(a.getName())));
    }

    @Test
    public void testNoQueueBacklogAlertWhenLow() {
        metricsCollector.setQueueSize(100); // 10% of 1000

        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertFalse(alerts.stream().anyMatch(a -> "QUEUE_BACKLOG".equals(a.getName())));
    }

    @Test
    public void testGetActiveAlerts() {
        metricsCollector.setContinuousFailDurationSeconds(601);
        evaluator.evaluate();

        List<AlertEvaluator.Alert> active = evaluator.getActiveAlerts();
        assertFalse(active.isEmpty());
    }

    @Test
    public void testAlertToString() {
        AlertEvaluator.Alert alert = new AlertEvaluator.Alert("TEST", "P0", "test msg");
        String str = alert.toString();
        assertTrue(str.contains("P0"));
        assertTrue(str.contains("TEST"));
        assertTrue(str.contains("test msg"));
    }

    @Test
    public void testAlertGetters() {
        AlertEvaluator.Alert alert = new AlertEvaluator.Alert("NAME", "P1", "msg");
        assertEquals("NAME", alert.getName());
        assertEquals("P1", alert.getLevel());
        assertEquals("msg", alert.getMessage());
        assertTrue(alert.getTimestamp() > 0);
    }

    @Test
    public void testStartAndStop() throws Exception {
        evaluator.start();
        Thread.sleep(100);
        evaluator.stop();
        // 不应抛出异常
    }

    @Test
    public void testMultipleAlerts() {
        metricsCollector.setContinuousFailDurationSeconds(601);
        metricsCollector.setTargetClusterStatus("UNAVAILABLE");
        metricsCollector.setParseErrorSuspendStatus("PARSE_ERROR_SUSPENDED");
        metricsCollector.setQueueSize(900);

        List<AlertEvaluator.Alert> alerts = evaluator.evaluate();
        assertTrue(alerts.size() >= 4);
    }

    @Test
    public void testIncrementalDelta() {
        // 第一轮评估
        for (int i = 0; i < 5; i++) {
            metricsCollector.incrementSyncFailureCount();
        }
        List<AlertEvaluator.Alert> alerts1 = evaluator.evaluate();
        // 5 次不触发告警（阈值 > 10）
        assertFalse(alerts1.stream().anyMatch(a -> "WRITE_ERROR_FREQUENT".equals(a.getName())));

        // 第二轮评估，增量只有 3 次
        for (int i = 0; i < 3; i++) {
            metricsCollector.incrementSyncFailureCount();
        }
        List<AlertEvaluator.Alert> alerts2 = evaluator.evaluate();
        assertFalse(alerts2.stream().anyMatch(a -> "WRITE_ERROR_FREQUENT".equals(a.getName())));
    }
}
