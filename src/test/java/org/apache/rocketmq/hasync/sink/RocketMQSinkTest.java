package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.checkpoint.CheckpointCoordinatorImpl;
import org.apache.rocketmq.hasync.config.SinkConfig;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * RocketMQSink 单元测试
 */
public class RocketMQSinkTest {

    private RocketMQSink sink;
    private SinkConfig config;
    private MetricsCollector metricsCollector;
    private CheckpointCoordinatorImpl checkpointCoordinator;
    private TopicFilter topicFilter;
    private TopicOnDemandSync topicOnDemandSync;
    private SinkRetryPolicy retryPolicy;
    private Map<String, String> kvStore;

    @Before
    public void setUp() {
        config = new SinkConfig();
        config.load(new String[]{"--targetNamesrv", "127.0.0.1:9876"});

        metricsCollector = new MetricsCollector();
        kvStore = new ConcurrentHashMap<>();

        checkpointCoordinator = new CheckpointCoordinatorImpl("broker-a", 1000, 100);
        checkpointCoordinator.setPersistCallback(new CheckpointCoordinatorImpl.CheckpointPersistCallback() {
            @Override
            public void putKVConfig(String ns, String key, String value) {
                kvStore.put(ns + ":" + key, value);
            }
            @Override
            public String getKVConfig(String ns, String key) {
                return kvStore.get(ns + ":" + key);
            }
        });

        topicFilter = new TopicFilter(null); // 不过滤
        topicOnDemandSync = new TopicOnDemandSync(3);
        topicOnDemandSync.setSyncCallback(topic -> true); // 总是同步成功
        retryPolicy = new SinkRetryPolicy(3);
        retryPolicy.setMetricsCollector(metricsCollector);

        sink = new RocketMQSink(config, topicFilter, topicOnDemandSync,
                retryPolicy, checkpointCoordinator, metricsCollector);
    }

    @Test
    public void testStartAndStop() throws Exception {
        sink.start();
        assertTrue(sink.isRunning());
        sink.stop();
        assertFalse(sink.isRunning());
    }

    @Test
    public void testWriteSuccess() throws Exception {
        sink.setWriteCallback(new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                return "msg-001";
            }
            @Override
            public boolean probe() { return true; }
        });

        sink.start();

        SyncRecord record = createRecord("TestTopic", 1000L, 1100L, 0);
        sink.write(record);

        assertEquals(1, metricsCollector.getSyncSuccessCount());
    }

    @Test
    public void testTopicFiltering() throws Exception {
        // 启用过滤，只允许 TopicA
        TopicFilter filter = new TopicFilter(new HashSet<String>() {{ add("TopicA"); }});
        filter.setMetricsCollector(metricsCollector);
        RocketMQSink filteredSink = new RocketMQSink(config, filter, topicOnDemandSync,
                retryPolicy, checkpointCoordinator, metricsCollector);
        filteredSink.setWriteCallback(new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                return "ok";
            }
            @Override
            public boolean probe() { return true; }
        });

        filteredSink.start();

        // 写入 TopicB → 被过滤
        filteredSink.write(createRecord("TopicB", 1000L, 1100L, 0));
        assertEquals(1, metricsCollector.getFilteredMessageCount());

        // 写入 TopicA → 通过
        filteredSink.write(createRecord("TopicA", 1100L, 1200L, 0));
        assertEquals(1, metricsCollector.getSyncSuccessCount());

        filteredSink.stop();
    }

    @Test
    public void testWriteFailureMarksUnavailable() throws Exception {
        AtomicInteger failCount = new AtomicInteger(0);
        sink.setWriteCallback(new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) throws Exception {
                failCount.incrementAndGet();
                throw new SinkRetryPolicy.RetryableException("always fail");
            }
            @Override
            public boolean probe() { return false; }
        });

        sink.start();

        // 写入 11 次失败（超过阈值 10）
        for (int i = 0; i < 11; i++) {
            try {
                sink.write(createRecord("TestTopic", 1000L + i * 100, 1100L + i * 100, 0));
            } catch (Exception e) {
                // 预期异常
            }
        }

        // 确认连续失败超过阈值
        assertTrue(sink.getContinuousFailCount() >= 10);

        sink.stop();
    }

    @Test
    public void testProbeTargetCluster() throws Exception {
        sink.setWriteCallback(new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                return "ok";
            }
            @Override
            public boolean probe() { return true; }
        });

        sink.start();
        assertTrue(sink.probeTargetCluster());
        assertEquals(1, metricsCollector.getTargetProbeSuccessCount());
        sink.stop();
    }

    @Test
    public void testProbeTargetClusterFails() throws Exception {
        sink.setWriteCallback(new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                return "ok";
            }
            @Override
            public boolean probe() { return false; }
        });

        sink.start();
        assertFalse(sink.probeTargetCluster());
        assertEquals(1, metricsCollector.getTargetProbeFailureCount());
        sink.stop();
    }

    @Test
    public void testProbeNoCallback() throws Exception {
        sink.start();
        assertFalse(sink.probeTargetCluster());
        sink.stop();
    }

    @Test
    public void testFlush() throws Exception {
        sink.start();
        sink.flush(); // 不应抛出异常
        sink.stop();
    }

    @Test
    public void testGetters() throws Exception {
        sink.start();
        assertNotNull(sink.getTopicFilter());
        assertNotNull(sink.getTopicOnDemandSync());
        assertNotNull(sink.getRetryPolicy());
        assertNotNull(sink.getCheckpointCoordinator());
        assertEquals("AVAILABLE", sink.getTargetClusterStatus());
        assertEquals("RUNNING", sink.getSyncStatus());
        assertEquals(0, sink.getTargetUnavailableDurationSeconds());
        sink.stop();
    }

    @Test
    public void testWriteWhenNotRunning() throws Exception {
        // Sink 未启动时写入应该被忽略
        SyncRecord record = createRecord("TestTopic", 1000L, 1100L, 0);
        sink.write(record);
        assertEquals(0, metricsCollector.getSyncSuccessCount());
    }

    // ==================== 辅助方法 ====================

    private SyncRecord createRecord(String topic, long physicOffset, long endOffset, int queueId) {
        SyncRecord record = new SyncRecord();
        record.setTopic(topic);
        record.setPhysicOffset(physicOffset);
        record.setEndOffset(endOffset);
        record.setQueueId(queueId);
        record.setBody(new byte[64]);
        record.setTraceId("trace-" + physicOffset);
        return record;
    }
}
