package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.*;

/**
 * TopicOnDemandSync 单元测试
 */
public class TopicOnDemandSyncTest {

    private TopicOnDemandSync sync;
    private MetricsCollector metricsCollector;

    @Before
    public void setUp() {
        metricsCollector = new MetricsCollector();
        sync = new TopicOnDemandSync(3);
        sync.setMetricsCollector(metricsCollector);
    }

    @Test
    public void testCachedTopicReturnsTrue() {
        sync.initCache(new HashSet<>(Arrays.asList("TopicA", "TopicB")));
        assertTrue(sync.ensureTopicExists("TopicA"));
        assertTrue(sync.ensureTopicExists("TopicB"));
    }

    @Test
    public void testSyncSuccess() {
        sync.setSyncCallback(topic -> true);
        assertTrue(sync.ensureTopicExists("NewTopic"));
        assertTrue(sync.getLocalTopicCache().contains("NewTopic"));
    }

    @Test
    public void testSyncFailureWithRetry() {
        // 前 3 次失败，第 4 次也失败（超过 maxRetry=3）
        sync.setSyncCallback(topic -> {
            throw new RuntimeException("模拟失败");
        });

        assertFalse(sync.ensureTopicExists("FailTopic"));
        assertTrue(sync.isSuspended());
        assertEquals("FailTopic", sync.getFailedTopic());
    }

    @Test
    public void testSuspendedBlocksOtherTopics() {
        sync.setSyncCallback(topic -> {
            throw new RuntimeException("模拟失败");
        });

        sync.ensureTopicExists("FailTopic"); // 触发暂停
        assertFalse(sync.ensureTopicExists("AnotherTopic")); // 被暂停阻塞
    }

    @Test
    public void testResume() {
        sync.setSyncCallback(topic -> {
            throw new RuntimeException("模拟失败");
        });

        sync.ensureTopicExists("FailTopic"); // 触发暂停
        assertTrue(sync.isSuspended());

        sync.resume();
        assertFalse(sync.isSuspended());
        assertEquals("", sync.getFailedTopic());
    }

    @Test
    public void testNoCallbackReturnsFalse() {
        assertFalse(sync.ensureTopicExists("TopicX"));
    }

    @Test
    public void testMetricsOnDemandCount() {
        sync.setSyncCallback(topic -> true);
        sync.ensureTopicExists("NewTopic1");
        sync.ensureTopicExists("NewTopic2");
        assertEquals(2, metricsCollector.getTopicSyncOnDemandCount());
    }

    @Test
    public void testMetricsFailureCount() {
        sync.setSyncCallback(topic -> {
            throw new RuntimeException("fail");
        });
        sync.ensureTopicExists("FailTopic");
        assertEquals(1, metricsCollector.getTopicSyncFailureCount());
    }

    @Test
    public void testInitCache() {
        sync.initCache(new HashSet<>(Arrays.asList("T1", "T2", "T3")));
        assertEquals(3, sync.getLocalTopicCache().size());
    }

    @Test
    public void testInitCacheNull() {
        sync.initCache(null);
        assertEquals(0, sync.getLocalTopicCache().size());
    }

    @Test
    public void testStop() {
        sync.stop(); // 不应抛出异常
    }

    @Test
    public void testGetMaxRetry() {
        assertEquals(3, sync.getMaxRetry());
    }

    @Test
    public void testSyncSuccessOnRetry() {
        final int[] callCount = {0};
        sync.setSyncCallback(topic -> {
            callCount[0]++;
            if (callCount[0] < 3) {
                throw new RuntimeException("前两次失败");
            }
            return true;
        });

        assertTrue(sync.ensureTopicExists("RetryTopic"));
        assertTrue(sync.getLocalTopicCache().contains("RetryTopic"));
        assertFalse(sync.isSuspended());
    }
}
