package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.checkpoint.CheckpointCoordinatorImpl;
import org.apache.rocketmq.hasync.config.SinkConfig;
import org.apache.rocketmq.hasync.core.TestSyncPipelineHelper;
import org.apache.rocketmq.hasync.core.SyncSink;
import org.apache.rocketmq.hasync.core.SyncSource;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Sink 扩容测试 — 验证动态增加 Sink 实例、队列扩容后的行为
 * <p>
 * 覆盖场景：
 * <ul>
 *   <li>多 Sink 并行写入的正确性</li>
 *   <li>扩容后新 Sink 的 Checkpoint 独立管理</li>
 *   <li>队列数增加后 FixedQueueSelector 的映射正确性</li>
 *   <li>扩容过程中数据不丢失、不重复</li>
 *   <li>扩容后各 Sink 负载均衡</li>
 *   <li>目标集群队列扩容（4→8）的写入行为</li>
 * </ul>
 */
public class SinkScaleUpTest {

    private SinkConfig config;
    private MetricsCollector metricsCollector;
    private Map<String, String> kvStore;

    @Before
    public void setUp() {
        config = new SinkConfig();
        config.load(new String[]{"--targetNamesrv", "127.0.0.1:9876"});
        metricsCollector = new MetricsCollector();
        kvStore = new ConcurrentHashMap<>();
    }

    @After
    public void tearDown() {
        metricsCollector.reset();
    }

    // ==================== 多 Sink 并行写入测试 ====================

    /**
     * 测试扩容：从 1 个 Sink 扩展到 3 个 Sink，验证每个 Sink 都能独立写入
     */
    @Test
    public void testScaleUpFromOneToThreeSinks() throws Exception {
        // 阶段 1：1 个 Sink 运行
        RocketMQSink sink1 = createSink("sink-1");
        List<String> sink1Msgs = new CopyOnWriteArrayList<>();
        sink1.setWriteCallback(createTrackingCallback(sink1Msgs));
        sink1.start();

        // 写入 5 条消息
        for (int i = 0; i < 5; i++) {
            sink1.write(createRecord("TopicA", 1000L + i * 100, 1100L + i * 100, i % 4));
        }
        assertEquals("Sink-1 应收到 5 条消息", 5, sink1Msgs.size());

        // 阶段 2：扩容到 3 个 Sink
        RocketMQSink sink2 = createSink("sink-2");
        RocketMQSink sink3 = createSink("sink-3");
        List<String> sink2Msgs = new CopyOnWriteArrayList<>();
        List<String> sink3Msgs = new CopyOnWriteArrayList<>();
        sink2.setWriteCallback(createTrackingCallback(sink2Msgs));
        sink3.setWriteCallback(createTrackingCallback(sink3Msgs));
        sink2.start();
        sink3.start();

        // 验证所有 3 个 Sink 都在运行
        assertTrue("Sink-1 应在运行", sink1.isRunning());
        assertTrue("Sink-2 应在运行", sink2.isRunning());
        assertTrue("Sink-3 应在运行", sink3.isRunning());

        // 每个 Sink 再各写入 3 条消息
        for (int i = 0; i < 3; i++) {
            long offset = 2000L + i * 100;
            sink1.write(createRecord("TopicA", offset, offset + 100, 0));
            sink2.write(createRecord("TopicA", offset + 10000, offset + 10100, 1));
            sink3.write(createRecord("TopicA", offset + 20000, offset + 20100, 2));
        }

        assertEquals("Sink-1 应累计收到 8 条消息", 8, sink1Msgs.size());
        assertEquals("Sink-2 应收到 3 条消息", 3, sink2Msgs.size());
        assertEquals("Sink-3 应收到 3 条消息", 3, sink3Msgs.size());

        sink1.stop();
        sink2.stop();
        sink3.stop();
    }

    // ==================== Checkpoint 独立管理测试 ====================

    /**
     * 测试扩容后每个 Sink 的 Checkpoint 独立推进、互不影响
     */
    @Test
    public void testScaleUpCheckpointIndependence() throws Exception {
        CheckpointCoordinatorImpl coordinator = createCheckpointCoordinator();
        coordinator.start();

        RocketMQSink sink1 = createSinkWithCoordinator("sink-A", coordinator);
        RocketMQSink sink2 = createSinkWithCoordinator("sink-B", coordinator);

        sink1.setWriteCallback(createSuccessCallback());
        sink2.setWriteCallback(createSuccessCallback());

        sink1.start();
        sink2.start();

        // Sink-A 写入 5 条，endOffset 从 2000 到 6000
        for (int i = 0; i < 5; i++) {
            long offset = 1000L + i * 1000;
            sink1.write(createRecord("TopicA", offset, offset + 1000, 0));
        }

        // Sink-B 写入 3 条，endOffset 从 2000 到 4000
        for (int i = 0; i < 3; i++) {
            long offset = 1000L + i * 1000;
            sink2.write(createRecord("TopicA", offset, offset + 1000, 1));
        }

        coordinator.flush();

        // 验证各 Sink 独立的 commitOffset（commitOffset = 最后一条的 endOffset）
        Long sinkAOffset = coordinator.getSinkCommitOffsets().get("sink-A");
        Long sinkBOffset = coordinator.getSinkCommitOffsets().get("sink-B");
        assertNotNull("Sink-A 应有 commitOffset", sinkAOffset);
        assertNotNull("Sink-B 应有 commitOffset", sinkBOffset);
        assertEquals("Sink-A 的 commitOffset 应为最后一条的 endOffset 6000", 6000L, sinkAOffset.longValue());
        assertEquals("Sink-B 的 commitOffset 应为最后一条的 endOffset 4000", 4000L, sinkBOffset.longValue());

        // globalCheckpoint = min(6000, 4000) = 4000
        assertEquals("全局 Checkpoint 应为两个 Sink 中的最小值", 4000L, coordinator.getGlobalCheckpoint());

        sink1.stop();
        sink2.stop();
        coordinator.stop();
    }

    /**
     * 测试扩容新增 Sink 时，Checkpoint 从 0 开始（新 Sink 无历史位点）
     */
    @Test
    public void testNewSinkCheckpointStartsFromZero() throws Exception {
        CheckpointCoordinatorImpl coordinator = createCheckpointCoordinator();
        coordinator.start();

        // 新增一个从未运行过的 Sink
        long recovered = coordinator.recoverCheckpoint("new-sink-001");
        assertEquals("新 Sink 的恢复 offset 应为 0", 0L, recovered);

        coordinator.stop();
    }

    // ==================== 队列扩容映射测试 ====================

    /**
     * 测试目标集群队列从 4 扩容到 8 后，FixedQueueSelector 的映射正确性
     */
    @Test
    public void testQueueScaleUpSelectorMapping() {
        FixedQueueSelector selector = new FixedQueueSelector();

        // 扩容前：4 个队列
        int oldQueueNum = 4;
        // 扩容后：8 个队列
        int newQueueNum = 8;

        // 扩容前：queueId 0~3 直接映射
        for (int q = 0; q < 4; q++) {
            assertEquals("扩容前 queueId " + q + " 应直接映射", q, selector.select(oldQueueNum, q));
        }

        // 扩容后：queueId 0~7 都可直接映射
        for (int q = 0; q < 8; q++) {
            assertEquals("扩容后 queueId " + q + " 应直接映射", q, selector.select(newQueueNum, q));
        }

        // 扩容后，原先需要取模的 queueId 现在可以直接映射
        // 例如 queueId=5，扩容前（4队列）→ 5%4=1，扩容后（8队列）→ 5
        assertEquals("扩容前 queueId 5 应映射到 1", 1, selector.select(oldQueueNum, 5));
        assertEquals("扩容后 queueId 5 应映射到 5", 5, selector.select(newQueueNum, 5));
    }

    /**
     * 测试队列扩容前后的配置一致性检查
     */
    @Test
    public void testQueueConfigConsistencyAfterScaleUp() {
        FixedQueueSelector selector = new FixedQueueSelector();

        // 源集群 8 队列，目标扩容前 4 队列
        assertFalse("扩容前配置不一致", selector.isQueueConfigConsistent(8, 4));

        // 目标扩容后 8 队列
        assertTrue("扩容后配置一致", selector.isQueueConfigConsistent(8, 8));

        // 目标扩容到 16 队列（过度扩容）
        assertFalse("过度扩容配置不一致", selector.isQueueConfigConsistent(8, 16));
    }

    /**
     * 测试队列扩容（4→8）后消息分布更均匀
     */
    @Test
    public void testQueueScaleUpImprovesMsgDistribution() {
        FixedQueueSelector selector = new FixedQueueSelector();

        // 模拟源集群 8 个队列产生的消息
        int[] sourceQueueIds = {0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7};

        // 扩容前：目标 4 个队列 → 多个源队列映射到同一目标队列
        int[] dist4 = new int[4];
        for (int qid : sourceQueueIds) {
            dist4[selector.select(4, qid)]++;
        }
        // queueId 0和4 都映射到 0，1和5→1，2和6→2，3和7→3
        for (int d : dist4) {
            assertEquals("扩容前每个目标队列应分到 4 条", 4, d);
        }

        // 扩容后：目标 8 个队列 → 一对一映射
        int[] dist8 = new int[8];
        for (int qid : sourceQueueIds) {
            dist8[selector.select(8, qid)]++;
        }
        // 每个目标队列恰好 2 条
        for (int d : dist8) {
            assertEquals("扩容后每个目标队列应分到 2 条", 2, d);
        }
    }

    // ==================== 扩容数据完整性测试 ====================

    /**
     * 测试扩容过程中正在写入的数据不丢失
     */
    @Test
    public void testScaleUpNoDataLoss() throws Exception {
        final int totalMsgs = 100;
        final AtomicInteger globalWriteCount = new AtomicInteger(0);

        // 第 1 阶段：只有 1 个 Sink
        RocketMQSink sink1 = createSink("sink-1");
        sink1.setWriteCallback(createCountingCallback(globalWriteCount));
        sink1.start();

        // 写入前 50 条
        for (int i = 0; i < 50; i++) {
            sink1.write(createRecord("TopicA", i * 100, (i + 1) * 100, i % 4));
        }
        assertEquals("前 50 条应全部写入", 50, globalWriteCount.get());

        // 第 2 阶段：扩容增加 Sink-2
        RocketMQSink sink2 = createSink("sink-2");
        sink2.setWriteCallback(createCountingCallback(globalWriteCount));
        sink2.start();

        // 写入后 50 条（分配给两个 Sink）
        for (int i = 50; i < totalMsgs; i++) {
            SyncRecord record = createRecord("TopicA", i * 100, (i + 1) * 100, i % 4);
            if (i % 2 == 0) {
                sink1.write(record);
            } else {
                sink2.write(record);
            }
        }

        assertEquals("全部 100 条消息应完整写入，无丢失", totalMsgs, globalWriteCount.get());

        sink1.stop();
        sink2.stop();
    }

    /**
     * 测试扩容过程中不产生重复写入
     */
    @Test
    public void testScaleUpNoDuplicateWrites() throws Exception {
        CopyOnWriteArrayList<Long> allOffsets = new CopyOnWriteArrayList<>();

        RocketMQSink sink1 = createSink("sink-1");
        RocketMQSink sink2 = createSink("sink-2");

        RocketMQSink.SinkWriteCallback offsetTracker = new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                String offsetStr = properties.get("ORIGIN_PHYSICAL_OFFSET");
                if (offsetStr != null) {
                    allOffsets.add(Long.parseLong(offsetStr));
                }
                return "ok";
            }
            @Override
            public boolean probe() { return true; }
        };

        sink1.setWriteCallback(offsetTracker);
        sink2.setWriteCallback(offsetTracker);
        sink1.start();
        sink2.start();

        // 每个 Sink 写入不同的 offset 范围
        for (int i = 0; i < 20; i++) {
            sink1.write(createRecord("TopicA", i * 100, (i + 1) * 100, 0));
        }
        for (int i = 20; i < 40; i++) {
            sink2.write(createRecord("TopicA", i * 100, (i + 1) * 100, 1));
        }

        // 验证无重复 offset
        HashSet<Long> uniqueOffsets = new HashSet<>(allOffsets);
        assertEquals("不应有重复写入的 offset", allOffsets.size(), uniqueOffsets.size());
        assertEquals("总共应写入 40 条", 40, allOffsets.size());

        sink1.stop();
        sink2.stop();
    }

    // ==================== Pipeline 多 Sink 扩容测试 ====================

    /**
     * 测试 Pipeline 中配置多个 Sink（模拟扩容后状态），数据能被消费
     */
    @Test
    public void testPipelineWithMultipleSinksAfterScaleUp() throws Exception {
        final CountDownLatch allWritten = new CountDownLatch(10);
        final AtomicInteger sink1Count = new AtomicInteger(0);
        final AtomicInteger sink2Count = new AtomicInteger(0);

        SyncSink pSink1 = new SyncSink() {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public void flush() {}
            @Override
            public void write(SyncRecord record) {
                sink1Count.incrementAndGet();
                allWritten.countDown();
            }
        };
        SyncSink pSink2 = new SyncSink() {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public void flush() {}
            @Override
            public void write(SyncRecord record) {
                sink2Count.incrementAndGet();
                allWritten.countDown();
            }
        };

        MockSource source = new MockSource();
        TestSyncPipelineHelper pipeline = new TestSyncPipelineHelper(source, Arrays.<SyncSink>asList(pSink1, pSink2), 100);
        pipeline.start();

        // 向队列投递 10 条消息
        for (int i = 0; i < 10; i++) {
            SyncRecord r = createRecord("TopicA", i * 100, (i + 1) * 100, i % 4);
            pipeline.offer(r);
        }

        // 等待消费完成（两个 Sink 线程竞争消费）
        boolean done = allWritten.await(10, TimeUnit.SECONDS);
        assertTrue("所有消息应在超时前被消费", done);

        // 两个 Sink 线程总共应消费 10 条
        int total = sink1Count.get() + sink2Count.get();
        assertEquals("两个 Sink 总消费应为 10", 10, total);

        pipeline.stopAll();
    }

    // ==================== 扩容后各 Sink 的 Metrics 独立验证 ====================

    /**
     * 测试扩容后每个 Sink 使用独立的 MetricsCollector
     */
    @Test
    public void testScaleUpMetricsIndependence() throws Exception {
        MetricsCollector metrics1 = new MetricsCollector();
        MetricsCollector metrics2 = new MetricsCollector();

        RocketMQSink sink1 = createSinkWithMetrics("sink-m1", metrics1);
        RocketMQSink sink2 = createSinkWithMetrics("sink-m2", metrics2);

        sink1.setWriteCallback(createSuccessCallback());
        sink2.setWriteCallback(createSuccessCallback());

        sink1.start();
        sink2.start();

        // Sink-1 写入 5 条
        for (int i = 0; i < 5; i++) {
            sink1.write(createRecord("TopicA", i * 100, (i + 1) * 100, 0));
        }

        // Sink-2 写入 3 条
        for (int i = 0; i < 3; i++) {
            sink2.write(createRecord("TopicA", i * 100 + 10000, (i + 1) * 100 + 10000, 1));
        }

        assertEquals("Sink-1 的成功计数应为 5", 5, metrics1.getSyncSuccessCount());
        assertEquals("Sink-2 的成功计数应为 3", 3, metrics2.getSyncSuccessCount());
        assertEquals("Sink-1 的失败计数应为 0", 0, metrics1.getSyncFailureCount());
        assertEquals("Sink-2 的失败计数应为 0", 0, metrics2.getSyncFailureCount());

        sink1.stop();
        sink2.stop();
    }

    // ==================== 扩容：新 Sink 启动失败不影响已有 Sink ====================

    /**
     * 测试新增 Sink 启动失败时不影响已运行的 Sink
     */
    @Test
    public void testNewSinkStartFailureDoesNotAffectExisting() throws Exception {
        RocketMQSink sink1 = createSink("sink-ok");
        List<String> sink1Msgs = new CopyOnWriteArrayList<>();
        sink1.setWriteCallback(createTrackingCallback(sink1Msgs));
        sink1.start();
        assertTrue("Sink-1 应在运行", sink1.isRunning());

        // 写入正常
        sink1.write(createRecord("TopicA", 1000L, 1100L, 0));
        assertEquals(1, sink1Msgs.size());

        // 扩容的 Sink-2 在 start 后设置了错误 callback（模拟启动后异常）
        RocketMQSink sink2 = createSink("sink-fail");
        sink2.setWriteCallback(new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) throws Exception {
                throw new RuntimeException("模拟新 Sink 写入失败");
            }
            @Override
            public boolean probe() { return false; }
        });
        sink2.start();

        // Sink-2 写入失败不应影响 Sink-1
        try {
            sink2.write(createRecord("TopicA", 2000L, 2100L, 0));
        } catch (Exception e) {
            // 预期异常
        }

        // Sink-1 仍正常写入
        sink1.write(createRecord("TopicA", 3000L, 3100L, 0));
        assertEquals("Sink-1 应继续正常工作", 2, sink1Msgs.size());
        assertTrue("Sink-1 应仍在运行", sink1.isRunning());

        sink1.stop();
        sink2.stop();
    }

    // ==================== 大规模扩容测试 ====================

    /**
     * 测试从 1 个 Sink 扩容到 10 个 Sink 的并发写入
     */
    @Test
    public void testLargeScaleUp() throws Exception {
        int sinkCount = 10;
        int msgsPerSink = 20;
        AtomicInteger totalWrites = new AtomicInteger(0);

        List<RocketMQSink> sinks = new ArrayList<>();
        for (int s = 0; s < sinkCount; s++) {
            RocketMQSink sink = createSink("sink-" + s);
            sink.setWriteCallback(createCountingCallback(totalWrites));
            sink.start();
            sinks.add(sink);
        }

        // 每个 Sink 写入 20 条
        for (int s = 0; s < sinkCount; s++) {
            for (int i = 0; i < msgsPerSink; i++) {
                long offset = s * 10000L + i * 100;
                sinks.get(s).write(createRecord("TopicA", offset, offset + 100, i % 8));
            }
        }

        assertEquals("10 个 Sink 各 20 条，总共 200 条", sinkCount * msgsPerSink, totalWrites.get());

        for (RocketMQSink sink : sinks) {
            sink.stop();
        }
    }

    // ==================== 辅助方法 ====================

    private RocketMQSink createSink(String sinkId) {
        CheckpointCoordinatorImpl coordinator = createCheckpointCoordinator();
        coordinator.start();

        TopicFilter topicFilter = new TopicFilter(null);
        TopicOnDemandSync topicOnDemandSync = new TopicOnDemandSync(3);
        topicOnDemandSync.setSyncCallback(topic -> true);
        SinkRetryPolicy retryPolicy = new SinkRetryPolicy(3);
        retryPolicy.setMetricsCollector(metricsCollector);

        // 覆盖 config 的 sinkId
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.load(new String[]{"--targetNamesrv", "127.0.0.1:9876", "--sinkId", sinkId});

        return new RocketMQSink(sinkConfig, topicFilter, topicOnDemandSync,
                retryPolicy, coordinator, metricsCollector);
    }

    private RocketMQSink createSinkWithCoordinator(String sinkId, CheckpointCoordinatorImpl coordinator) {
        TopicFilter topicFilter = new TopicFilter(null);
        TopicOnDemandSync topicOnDemandSync = new TopicOnDemandSync(3);
        topicOnDemandSync.setSyncCallback(topic -> true);
        SinkRetryPolicy retryPolicy = new SinkRetryPolicy(3);
        retryPolicy.setMetricsCollector(metricsCollector);

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.load(new String[]{"--targetNamesrv", "127.0.0.1:9876", "--sinkId", sinkId});

        return new RocketMQSink(sinkConfig, topicFilter, topicOnDemandSync,
                retryPolicy, coordinator, metricsCollector);
    }

    private RocketMQSink createSinkWithMetrics(String sinkId, MetricsCollector metrics) {
        CheckpointCoordinatorImpl coordinator = createCheckpointCoordinator();
        coordinator.start();

        TopicFilter topicFilter = new TopicFilter(null);
        TopicOnDemandSync topicOnDemandSync = new TopicOnDemandSync(3);
        topicOnDemandSync.setSyncCallback(topic -> true);
        SinkRetryPolicy retryPolicy = new SinkRetryPolicy(3);
        retryPolicy.setMetricsCollector(metrics);

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.load(new String[]{"--targetNamesrv", "127.0.0.1:9876", "--sinkId", sinkId});

        return new RocketMQSink(sinkConfig, topicFilter, topicOnDemandSync,
                retryPolicy, coordinator, metrics);
    }

    private CheckpointCoordinatorImpl createCheckpointCoordinator() {
        CheckpointCoordinatorImpl coordinator = new CheckpointCoordinatorImpl("broker-a", 1000, 100);
        coordinator.setPersistCallback(new CheckpointCoordinatorImpl.CheckpointPersistCallback() {
            @Override
            public void putKVConfig(String ns, String key, String value) {
                kvStore.put(ns + ":" + key, value);
            }
            @Override
            public String getKVConfig(String ns, String key) {
                return kvStore.get(ns + ":" + key);
            }
        });
        return coordinator;
    }

    private RocketMQSink.SinkWriteCallback createSuccessCallback() {
        return new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                return "ok-" + System.nanoTime();
            }
            @Override
            public boolean probe() { return true; }
        };
    }

    private RocketMQSink.SinkWriteCallback createTrackingCallback(final List<String> tracker) {
        return new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                String msgId = "msg-" + System.nanoTime();
                tracker.add(msgId);
                return msgId;
            }
            @Override
            public boolean probe() { return true; }
        };
    }

    private RocketMQSink.SinkWriteCallback createCountingCallback(final AtomicInteger counter) {
        return new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                counter.incrementAndGet();
                return "ok";
            }
            @Override
            public boolean probe() { return true; }
        };
    }

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

    /**
     * Mock SyncSource — 用于 Pipeline 测试
     */
    static class MockSource implements SyncSource {
        private volatile boolean running = false;

        @Override public void start() { running = true; }
        @Override public void stop() { running = false; }
        @Override public boolean isRunning() { return running; }
        @Override
        public void poll() {
            try { Thread.sleep(100); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
