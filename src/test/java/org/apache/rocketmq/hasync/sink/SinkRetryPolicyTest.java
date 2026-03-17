package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

/**
 * SinkRetryPolicy 单元测试
 */
public class SinkRetryPolicyTest {

    private SinkRetryPolicy retryPolicy;
    private MetricsCollector metricsCollector;

    @Before
    public void setUp() {
        metricsCollector = new MetricsCollector();
        retryPolicy = new SinkRetryPolicy(3);
        retryPolicy.setMetricsCollector(metricsCollector);
    }

    @Test
    public void testSuccessOnFirstAttempt() throws Exception {
        retryPolicy.setWriteExecutor((topic, body, queueId, props) -> "msg-001");

        String result = retryPolicy.sendWithRetry("TestTopic", new byte[10], 0, new HashMap<>());
        assertEquals("msg-001", result);
    }

    @Test
    public void testSuccessOnRetry() throws Exception {
        final int[] callCount = {0};
        retryPolicy.setWriteExecutor((topic, body, queueId, props) -> {
            callCount[0]++;
            if (callCount[0] < 3) {
                throw new SinkRetryPolicy.RetryableException("重试异常");
            }
            return "msg-retry";
        });

        String result = retryPolicy.sendWithRetry("TestTopic", new byte[10], 0, new HashMap<>());
        assertEquals("msg-retry", result);
        assertEquals(3, callCount[0]);
    }

    @Test(expected = SinkRetryPolicy.RetryableException.class)
    public void testAllRetriesFailed() throws Exception {
        retryPolicy.setWriteExecutor((topic, body, queueId, props) -> {
            throw new SinkRetryPolicy.RetryableException("always fail");
        });

        retryPolicy.sendWithRetry("TestTopic", new byte[10], 0, new HashMap<>());
    }

    @Test(expected = SinkRetryPolicy.NonRetryableException.class)
    public void testNonRetryableExceptionImmediateThrow() throws Exception {
        retryPolicy.setWriteExecutor((topic, body, queueId, props) -> {
            throw new SinkRetryPolicy.NonRetryableException("不可重试");
        });

        retryPolicy.sendWithRetry("TestTopic", new byte[10], 0, new HashMap<>());
    }

    @Test
    public void testMetricsOnFailure() {
        retryPolicy.setWriteExecutor((topic, body, queueId, props) -> {
            throw new SinkRetryPolicy.RetryableException("fail");
        });

        try {
            retryPolicy.sendWithRetry("TestTopic", new byte[10], 0, new HashMap<>());
        } catch (Exception e) {
            // 期望抛出异常
        }

        assertTrue(metricsCollector.getSyncFailureCount() > 0);
        assertTrue(metricsCollector.getStorageWriteErrorCount() > 0);
    }

    @Test
    public void testRetryCountMetrics() throws Exception {
        final int[] callCount = {0};
        retryPolicy.setWriteExecutor((topic, body, queueId, props) -> {
            callCount[0]++;
            if (callCount[0] < 2) {
                throw new RuntimeException("一般异常"); // 也会重试
            }
            return "ok";
        });

        retryPolicy.sendWithRetry("TestTopic", new byte[10], 0, new HashMap<>());
        assertTrue(metricsCollector.getRetryCount() > 0);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoWriteExecutor() throws Exception {
        SinkRetryPolicy noExecutor = new SinkRetryPolicy(3);
        noExecutor.sendWithRetry("Topic", new byte[10], 0, new HashMap<>());
    }

    @Test
    public void testGetMaxRetry() {
        assertEquals(3, retryPolicy.getMaxRetry());
    }

    @Test
    public void testZeroRetry() throws Exception {
        SinkRetryPolicy zeroRetry = new SinkRetryPolicy(0);
        zeroRetry.setMetricsCollector(metricsCollector);
        zeroRetry.setWriteExecutor((topic, body, queueId, props) -> "ok");

        String result = zeroRetry.sendWithRetry("Topic", new byte[10], 0, new HashMap<>());
        assertEquals("ok", result);
    }

    @Test
    public void testZeroRetryFails() {
        SinkRetryPolicy zeroRetry = new SinkRetryPolicy(0);
        zeroRetry.setMetricsCollector(metricsCollector);
        zeroRetry.setWriteExecutor((topic, body, queueId, props) -> {
            throw new SinkRetryPolicy.RetryableException("fail");
        });

        try {
            zeroRetry.sendWithRetry("Topic", new byte[10], 0, new HashMap<>());
            fail("应该抛出异常");
        } catch (Exception e) {
            // expected
        }
    }
}
