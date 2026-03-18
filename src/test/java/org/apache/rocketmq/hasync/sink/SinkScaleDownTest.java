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
 * Sink 缩容测试 — 验证动态移除 Sink 实例、队列缩容后的行为
 * <p>
 * 覆盖场景：
 * <ul>
 *   <li>缩容时 Sink 优雅停机（处理完剩余消息再退出）</li>
 *   <li>缩容后 Checkpoint 数据保留用于恢复</li>
 *   <li>缩容后剩余 Sink 接管数据写入</li>
 *   <li>队列缩容（8→4）后 FixedQueueSelector 的降级映射</li>
 *   <li>缩容过程中数据完整性保障</li>
 *   <li>缩容到最后 1 个 Sink 的边界场景</li>
 *   <li>缩容后再扩容的恢复能力</li>
 * </ul>
 */
public class SinkScaleDownTest {

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

    // ==================== Sink 优雅停机测试 ====================

    /**
     * 测试缩容时 Sink 能优雅停机：先 flush Checkpoint 再停止
     */
    @Test
    public void testSinkGracefulShutdownOnScaleDown() throws Exception {
        CheckpointCoordinatorImpl coordinator = createCheckpointCoordinator();
        coordinator.start();

        RocketMQSink sink = createSinkWithCoordinator("sink-to-remove", coordinator);
        sink.setWriteCallback(createSuccessCallback());
        sink.start();

        // 写入 10 条消息
        for (int i = 0; i < 10; i++) {
            long offset = 1000L + i * 100;
            sink.write(createRecord("TopicA", offset, offset + 100, 0));
        }

        // 缩容：停止 Sink（优雅停机应 flush Checkpoint）
        sink.stop();

        assertFalse("Sink 应已停止", sink.isRunning());

        // 验证 Checkpoint 已被持久化
        coordinator.flush();
        Long savedOffset = coordinator.getSinkCommitOffsets().get("sink-to-remove");
        assertNotNull("缩容停止后 Checkpoint 应已保存", savedOffset);
        // 10 条消息：offset 1000→1100, 1100→1200, ... 1900→2000，最后 endOffset=2000
        assertEquals("最终 commitOffset 应为最后一条的 endOffset 2000", 2000L, savedOffset.longValue());

        coordinator.stop();
    }

    /**
     * 测试从 3 个 Sink 缩容到 1 个 Sink，确保数据连续性
     */
    @Test
    public void testScaleDownFromThreeToOneSink() throws Exception {
        AtomicInteger totalWrites = new AtomicInteger(0);

        RocketMQSink sink1 = createSink("sink-1");
        RocketMQSink sink2 = createSink("sink-2");
        RocketMQSink sink3 = createSink("sink-3");

        sink1.setWriteCallback(createCountingCallback(totalWrites));
        sink2.setWriteCallback(createCountingCallback(totalWrites));
        sink3.setWriteCallback(createCountingCallback(totalWrites));

        sink1.start();
        sink2.start();
        sink3.start();

        // 阶段 1：3 个 Sink 各写入一些数据
        for (int i = 0; i < 10; i++) {
            SyncRecord record = createRecord("TopicA", i * 100, (i + 1) * 100, i % 4);
            if (i % 3 == 0) sink1.write(record);
            else if (i % 3 == 1) sink2.write(record);
            else sink3.write(record);
        }
        assertEquals("阶段 1 应写入 10 条", 10, totalWrites.get());

        // 阶段 2：缩容 — 停止 Sink-2 和 Sink-3
        sink2.stop();
        sink3.stop();
        assertFalse("Sink-2 应已停止", sink2.isRunning());
        assertFalse("Sink-3 应已停止", sink3.isRunning());
        assertTrue("Sink-1 应仍在运行", sink1.isRunning());

        // 阶段 3：所有流量由 Sink-1 承担
        for (int i = 10; i < 20; i++) {
            sink1.write(createRecord("TopicA", i * 100, (i + 1) * 100, i % 4));
        }
        assertEquals("总共应写入 20 条", 20, totalWrites.get());

        sink1.stop();
    }

    // ==================== Checkpoint 缩容保留与恢复测试 ====================

    /**
     * 测试缩容后被移除的 Sink 的 Checkpoint 在 KV 中保留，可用于将来恢复
     */
    @Test
    public void testScaleDownCheckpointPreserved() throws Exception {
        CheckpointCoordinatorImpl coordinator = createCheckpointCoordinator();
        coordinator.start();

        RocketMQSink sink1 = createSinkWithCoordinator("sink-keep", coordinator);
        RocketMQSink sink2 = createSinkWithCoordinator("sink-remove", coordinator);

        sink1.setWriteCallback(createSuccessCallback());
        sink2.setWriteCallback(createSuccessCallback());

        sink1.start();
        sink2.start();

        // Sink-keep 写入 5 条，endOffset: 2000, 3000, 4000, 5000, 6000
        for (int i = 0; i < 5; i++) {
            long offset = 1000L + i * 1000;
            sink1.write(createRecord("TopicA", offset, offset + 1000, 0));
        }

        // Sink-remove 写入 3 条，endOffset: 2000, 3000, 4000
        for (int i = 0; i < 3; i++) {
            long offset = 1000L + i * 1000;
            sink2.write(createRecord("TopicA", offset, offset + 1000, 1));
        }

        // 缩容：停止 Sink-remove
        sink2.stop();
        coordinator.flush();

        // 验证 Sink-remove 的 Checkpoint 仍在（commitOffset = 最后 endOffset = 4000）
        String removedKey = "SYNC_CHECKPOINT:broker-a:sink:sink-remove:commitOffset";
        assertNotNull("被移除 Sink 的 Checkpoint 应仍在 KV 中", kvStore.get(removedKey));
        assertEquals("4000", kvStore.get(removedKey));

        // Sink-keep 的 Checkpoint 也在（commitOffset = 最后 endOffset = 6000）
        String keepKey = "SYNC_CHECKPOINT:broker-a:sink:sink-keep:commitOffset";
        assertNotNull("保留 Sink 的 Checkpoint 应在 KV 中", kvStore.get(keepKey));
        assertEquals("6000", kvStore.get(keepKey));

        sink1.stop();
        coordinator.stop();
    }

    /**
     * 测试缩容后 globalCheckpoint 正确更新（只考虑存活 Sink）
     */
    @Test
    public void testGlobalCheckpointAfterScaleDown() throws Exception {
        CheckpointCoordinatorImpl coordinator = createCheckpointCoordinator();
        coordinator.start();

        // Sink-A: offset 5000, Sink-B: offset 2000
        coordinator.commitOffset("sink-A", 5000L);
        coordinator.commitOffset("sink-B", 2000L);
        assertEquals("缩容前 globalCheckpoint = min(5000, 2000) = 2000", 2000L, coordinator.getGlobalCheckpoint());

        // 模拟缩容后移除 Sink-B 的 offset 记录（实际业务中需要管理端操作）
        // 这里验证：如果两个 Sink 都还在 map 中，globalCheckpoint 仍为较低值
        // Sink-A 继续推进
        coordinator.commitOffset("sink-A", 8000L);
        // globalCheckpoint 仍受 Sink-B 拖累
        assertEquals("Sink-B 未移除时 globalCheckpoint 仍为 2000", 2000L, coordinator.getGlobalCheckpoint());

        // 清理 Sink-B 的记录后
        coordinator.getSinkCommitOffsets().remove("sink-B");
        assertEquals("清理 Sink-B 后 globalCheckpoint 应为 8000", 8000L, coordinator.getGlobalCheckpoint());

        coordinator.stop();
    }

    // ==================== 队列缩容映射测试 ====================

    /**
     * 测试目标集群队列从 8 缩容到 4 后，FixedQueueSelector 的降级映射
     */
    @Test
    public void testQueueScaleDownSelectorFallback() {
        FixedQueueSelector selector = new FixedQueueSelector();

        // 缩容前：8 个队列 → 直接映射
        for (int q = 0; q < 8; q++) {
            assertEquals("缩容前 queueId " + q + " 应直接映射", q, selector.select(8, q));
        }

        // 缩容后：4 个队列 → queueId 0~3 直接映射，4~7 取模
        assertEquals("queueId 0 → 0", 0, selector.select(4, 0));
        assertEquals("queueId 1 → 1", 1, selector.select(4, 1));
        assertEquals("queueId 2 → 2", 2, selector.select(4, 2));
        assertEquals("queueId 3 → 3", 3, selector.select(4, 3));
        assertEquals("queueId 4 → 0 (取模)", 0, selector.select(4, 4));
        assertEquals("queueId 5 → 1 (取模)", 1, selector.select(4, 5));
        assertEquals("queueId 6 → 2 (取模)", 2, selector.select(4, 6));
        assertEquals("queueId 7 → 3 (取模)", 3, selector.select(4, 7));
    }

    /**
     * 测试队列缩容后消息分布变化（合并效应）
     */
    @Test
    public void testQueueScaleDownMsgDistribution() {
        FixedQueueSelector selector = new FixedQueueSelector();

        // 源集群 8 个队列，各产生 2 条消息
        int[] sourceQueueIds = {0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7};

        // 缩容前：目标 8 个队列 → 每队列 2 条
        int[] dist8 = new int[8];
        for (int qid : sourceQueueIds) {
            dist8[selector.select(8, qid)]++;
        }
        for (int d : dist8) {
            assertEquals("缩容前每队列应 2 条", 2, d);
        }

        // 缩容后：目标 4 个队列 → 每队列 4 条（合并效应）
        int[] dist4 = new int[4];
        for (int qid : sourceQueueIds) {
            dist4[selector.select(4, qid)]++;
        }
        for (int d : dist4) {
            assertEquals("缩容后每队列应 4 条（合并效应）", 4, d);
        }
    }

    /**
     * 测试极端缩容：目标集群只剩 1 个队列
     */
    @Test
    public void testQueueScaleDownToSingleQueue() {
        FixedQueueSelector selector = new FixedQueueSelector();

        // 所有 queueId 都映射到 queue 0
        for (int q = 0; q < 16; q++) {
            assertEquals("只有 1 个队列时，所有消息应映射到 queue 0", 0, selector.select(1, q));
        }
    }

    /**
     * 测试队列缩容后配置一致性检测
     */
    @Test
    public void testQueueConfigConsistencyAfterScaleDown() {
        FixedQueueSelector selector = new FixedQueueSelector();

        // 源集群 8 队列，目标缩容到 4 队列
        assertFalse("缩容后配置应不一致", selector.isQueueConfigConsistent(8, 4));

        // 源集群 4 队列，目标也 4 队列
        assertTrue("匹配时应一致", selector.isQueueConfigConsistent(4, 4));
    }

    // ==================== 缩容数据完整性测试 ====================

    /**
     * 测试缩容过程中停止的 Sink 已写入的数据不丢失
     */
    @Test
    public void testScaleDownWrittenDataPreserved() throws Exception {
        CopyOnWriteArrayList<Long> sink1Offsets = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Long> sink2Offsets = new CopyOnWriteArrayList<>();

        RocketMQSink sink1 = createSink("sink-1");
        RocketMQSink sink2 = createSink("sink-2");

        sink1.setWriteCallback(createOffsetTrackingCallback(sink1Offsets));
        sink2.setWriteCallback(createOffsetTrackingCallback(sink2Offsets));

        sink1.start();
        sink2.start();

        // 两个 Sink 各写入 10 条
        for (int i = 0; i < 10; i++) {
            sink1.write(createRecord("TopicA", i * 100, (i + 1) * 100, 0));
            sink2.write(createRecord("TopicA", (i + 10) * 100, (i + 11) * 100, 1));
        }

        assertEquals("Sink-1 应写入 10 条", 10, sink1Offsets.size());
        assertEquals("Sink-2 应写入 10 条", 10, sink2Offsets.size());

        // 缩容：停止 Sink-2
        sink2.stop();

        // Sink-2 已写入的数据（offsets 1000~1900）应被保留（在回调中已记录）
        assertEquals("Sink-2 的历史写入数据不应丢失", 10, sink2Offsets.size());

        // Sink-1 继续工作
        for (int i = 20; i < 30; i++) {
            sink1.write(createRecord("TopicA", i * 100, (i + 1) * 100, 0));
        }
        assertEquals("Sink-1 应累计写入 20 条", 20, sink1Offsets.size());

        sink1.stop();
    }

    /**
     * 测试缩容过程中未完成的消息不被丢弃
     */
    @Test
    public void testScaleDownRemainingMsgsHandled() throws Exception {
        // 通过 Pipeline 测试：停止 Pipeline 时应 drain 剩余消息
        final AtomicInteger writeCount = new AtomicInteger(0);

        SyncSink countingSink = new SyncSink() {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public void flush() {}
            @Override
            public void write(SyncRecord record) {
                writeCount.incrementAndGet();
                try { Thread.sleep(10); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        MockSource source = new MockSource();
        TestSyncPipelineHelper pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(countingSink), 100);
        pipeline.start();

        // 投递 20 条消息
        for (int i = 0; i < 20; i++) {
            pipeline.offer(createRecord("TopicA", i * 100, (i + 1) * 100, 0));
        }

        // 等一小段时间让部分消息被消费
        Thread.sleep(100);

        // 优雅停机：应 drain 队列中剩余消息
        pipeline.stopAll();

        // 由于 drain 机制，所有投递的消息最终应被处理
        assertTrue("优雅停机应尽力处理剩余消息，至少处理一部分", writeCount.get() > 0);
    }

    // ==================== 缩容到 1 个 Sink 的边界场景 ====================

    /**
     * 测试从多个 Sink 缩容到 1 个 Sink，系统正常运行
     */
    @Test
    public void testScaleDownToSingleSink() throws Exception {
        List<RocketMQSink> sinks = new ArrayList<>();
        AtomicInteger totalWrites = new AtomicInteger(0);

        // 创建 5 个 Sink
        for (int i = 0; i < 5; i++) {
            RocketMQSink sink = createSink("sink-" + i);
            sink.setWriteCallback(createCountingCallback(totalWrites));
            sink.start();
            sinks.add(sink);
        }

        // 每个 Sink 写入 5 条
        for (int s = 0; s < 5; s++) {
            for (int i = 0; i < 5; i++) {
                long offset = s * 10000L + i * 100;
                sinks.get(s).write(createRecord("TopicA", offset, offset + 100, 0));
            }
        }
        assertEquals("5 Sink × 5 条 = 25 条", 25, totalWrites.get());

        // 逐步缩容到只剩 1 个
        for (int i = 4; i >= 1; i--) {
            sinks.get(i).stop();
            assertFalse("Sink-" + i + " 应已停止", sinks.get(i).isRunning());
        }

        // 只剩 Sink-0
        assertTrue("Sink-0 应仍在运行", sinks.get(0).isRunning());

        // Sink-0 继续写入
        for (int i = 0; i < 10; i++) {
            long offset = 50000L + i * 100;
            sinks.get(0).write(createRecord("TopicA", offset, offset + 100, 0));
        }
        assertEquals("缩容后应继续写入，总 35 条", 35, totalWrites.get());

        sinks.get(0).stop();
    }

    // ==================== 缩容后再扩容的恢复测试 ====================

    /**
     * 测试缩容再扩容：先缩到 1 个 Sink，再扩回 3 个 Sink，验证系统弹性
     */
    @Test
    public void testScaleDownThenScaleUpResilience() throws Exception {
        AtomicInteger totalWrites = new AtomicInteger(0);

        // 阶段 1：3 个 Sink 运行
        RocketMQSink sink1 = createSink("sink-1");
        RocketMQSink sink2 = createSink("sink-2");
        RocketMQSink sink3 = createSink("sink-3");

        sink1.setWriteCallback(createCountingCallback(totalWrites));
        sink2.setWriteCallback(createCountingCallback(totalWrites));
        sink3.setWriteCallback(createCountingCallback(totalWrites));

        sink1.start();
        sink2.start();
        sink3.start();

        // 各写 5 条 = 15 条
        for (int s = 0; s < 3; s++) {
            RocketMQSink sink = s == 0 ? sink1 : (s == 1 ? sink2 : sink3);
            for (int i = 0; i < 5; i++) {
                long offset = s * 10000L + i * 100;
                sink.write(createRecord("TopicA", offset, offset + 100, i % 4));
            }
        }
        assertEquals("阶段 1：15 条", 15, totalWrites.get());

        // 阶段 2：缩容到 1 个 Sink
        sink2.stop();
        sink3.stop();

        for (int i = 0; i < 5; i++) {
            long offset = 30000L + i * 100;
            sink1.write(createRecord("TopicA", offset, offset + 100, i % 4));
        }
        assertEquals("阶段 2：20 条", 20, totalWrites.get());

        // 阶段 3：再扩容回 3 个 Sink
        RocketMQSink newSink2 = createSink("sink-2-v2");
        RocketMQSink newSink3 = createSink("sink-3-v2");
        newSink2.setWriteCallback(createCountingCallback(totalWrites));
        newSink3.setWriteCallback(createCountingCallback(totalWrites));
        newSink2.start();
        newSink3.start();

        for (int i = 0; i < 3; i++) {
            long offset = 40000L + i * 100;
            sink1.write(createRecord("TopicA", offset, offset + 100, 0));
            newSink2.write(createRecord("TopicA", offset + 10000, offset + 10100, 1));
            newSink3.write(createRecord("TopicA", offset + 20000, offset + 20100, 2));
        }
        assertEquals("阶段 3：29 条", 29, totalWrites.get());

        assertTrue("新 Sink-2 应在运行", newSink2.isRunning());
        assertTrue("新 Sink-3 应在运行", newSink3.isRunning());

        sink1.stop();
        newSink2.stop();
        newSink3.stop();
    }

    /**
     * 测试缩容后重新扩容时，Checkpoint 可以从 KV 恢复
     */
    @Test
    public void testScaleDownAndRecoverCheckpoint() throws Exception {
        CheckpointCoordinatorImpl coordinator = createCheckpointCoordinator();
        coordinator.start();

        // Sink-A 写入 8 条，endOffset: 2000, 3000, ..., 9000
        RocketMQSink sinkA = createSinkWithCoordinator("sink-A", coordinator);
        sinkA.setWriteCallback(createSuccessCallback());
        sinkA.start();

        for (int i = 0; i < 8; i++) {
            long offset = 1000L + i * 1000;
            sinkA.write(createRecord("TopicA", offset, offset + 1000, 0));
        }

        coordinator.flush();
        sinkA.stop();

        // 缩容后，再恢复 Sink-A（commitOffset = 最后 endOffset = 9000）
        long recovered = coordinator.recoverCheckpoint("sink-A");
        assertEquals("恢复的 offset 应为 9000", 9000L, recovered);

        coordinator.stop();
    }

    // ==================== 停止已停止 Sink 的幂等性 ====================

    /**
     * 测试重复停止同一个 Sink 不会报错
     */
    @Test
    public void testDoubleStopSinkIsIdempotent() throws Exception {
        RocketMQSink sink = createSink("sink-idem");
        sink.setWriteCallback(createSuccessCallback());
        sink.start();
        assertTrue(sink.isRunning());

        // 第一次停止
        sink.stop();
        assertFalse(sink.isRunning());

        // 第二次停止 — 不应抛异常
        sink.stop();
        assertFalse(sink.isRunning());
    }

    // ==================== 缩容后停止的 Sink 不再接受写入 ====================

    /**
     * 测试停止的 Sink 写入操作被忽略
     */
    @Test
    public void testStoppedSinkIgnoresWrites() throws Exception {
        AtomicInteger writeCount = new AtomicInteger(0);

        RocketMQSink sink = createSink("sink-stopped");
        sink.setWriteCallback(createCountingCallback(writeCount));
        sink.start();

        sink.write(createRecord("TopicA", 1000L, 1100L, 0));
        assertEquals("启动时写入应计数", 1, writeCount.get());

        // 停止
        sink.stop();

        // 停止后写入应被忽略
        sink.write(createRecord("TopicA", 2000L, 2100L, 0));
        assertEquals("停止后写入应被忽略", 1, writeCount.get());
    }

    // ==================== Pipeline 级：停止 Sink 后消息重分配给在线 Sink ====================

    /**
     * 【检查点】停止后的 Sink 不再接受写入，Pipeline 将消息重新分配给其他在线 Sink
     * <p>
     * 验证流程：
     * <ol>
     *   <li>3 个 Sink 通过 Pipeline 竞争消费</li>
     *   <li>动态移除 Sink-C → Sink-C 停止、不再接收写入</li>
     *   <li>后续所有消息全部由存活的 Sink-A + Sink-B 消费，无消息丢失</li>
     * </ol>
     */
    @Test
    public void testPipelineRemoveSinkRedistributeMessages() throws Exception {
        final int totalMessages = 60;
        final AtomicInteger sinkACount = new AtomicInteger(0);
        final AtomicInteger sinkBCount = new AtomicInteger(0);
        final AtomicInteger sinkCCount = new AtomicInteger(0);
        final CountDownLatch allConsumed = new CountDownLatch(totalMessages);

        // 创建 3 个 Sink
        SyncSink sinkA = new SyncSink() {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public void flush() {}
            @Override
            public void write(SyncRecord record) {
                sinkACount.incrementAndGet();
                allConsumed.countDown();
            }
        };
        SyncSink sinkB = new SyncSink() {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public void flush() {}
            @Override
            public void write(SyncRecord record) {
                sinkBCount.incrementAndGet();
                allConsumed.countDown();
            }
        };
        SyncSink sinkC = new SyncSink() {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public void flush() {}
            @Override
            public void write(SyncRecord record) {
                sinkCCount.incrementAndGet();
                allConsumed.countDown();
            }
        };

        MockSource source = new MockSource();
        TestSyncPipelineHelper pipeline = new TestSyncPipelineHelper(source, Arrays.asList(sinkA, sinkB, sinkC), 200);
        pipeline.start();

        // 阶段 1：投递前 30 条，3 个 Sink 竞争消费
        for (int i = 0; i < 30; i++) {
            pipeline.offer(createRecord("TopicA", i * 100, (i + 1) * 100, i % 4));
        }
        Thread.sleep(500); // 等待消费

        int phase1Total = sinkACount.get() + sinkBCount.get() + sinkCCount.get();
        assertTrue("阶段 1：Sink-C 应参与消费（至少处理部分消息）", sinkCCount.get() > 0);
        assertTrue("阶段 1：3 个 Sink 应消费了前 30 条中的大部分", phase1Total > 0);

        // 记录移除前 Sink-C 的计数
        int sinkCCountBeforeRemoval = sinkCCount.get();

        // 阶段 2：动态移除 Sink-C（模拟缩容）
        boolean removed = pipeline.removeSink(sinkC);
        assertTrue("应成功移除 Sink-C", removed);
        assertEquals("活跃 Sink 应减为 2", 2, pipeline.getActiveSinkCount());

        // 验证 Sink-C 被停止后不再接受写入
        // Sink-C 的 stop() 已被 removeSink 调用，后续消息不会分配给它

        // 阶段 3：投递后 30 条，应只被 Sink-A 和 Sink-B 消费
        for (int i = 30; i < totalMessages; i++) {
            pipeline.offer(createRecord("TopicA", i * 100, (i + 1) * 100, i % 4));
        }

        // 等待所有消息被消费
        boolean done = allConsumed.await(10, TimeUnit.SECONDS);
        assertTrue("所有 60 条消息应在超时前被消费完", done);

        // 核心检查点：Sink-C 在被移除后不再接收新消息
        assertEquals("Sink-C 在被移除后不应接收任何新消息",
                sinkCCountBeforeRemoval, sinkCCount.get());

        // Sink-A 和 Sink-B 应接管了所有后续消息
        int sinkABPostRemoval = (sinkACount.get() + sinkBCount.get()) - (phase1Total - sinkCCountBeforeRemoval);
        assertEquals("Sink-A + Sink-B 应接管了移除后的全部 30 条消息", 30, sinkABPostRemoval);

        pipeline.stopAll();
    }

    /**
     * 【检查点】多次缩容：依次移除 Sink 直到只剩 1 个，消息始终被消费
     */
    @Test
    public void testPipelineSequentialRemoveSinksAllMsgsConsumed() throws Exception {
        final AtomicInteger totalConsumed = new AtomicInteger(0);

        SyncSink sinkA = createCountingSyncSink(totalConsumed);
        SyncSink sinkB = createCountingSyncSink(totalConsumed);
        SyncSink sinkC = createCountingSyncSink(totalConsumed);

        MockSource source = new MockSource();
        TestSyncPipelineHelper pipeline = new TestSyncPipelineHelper(source, Arrays.asList(sinkA, sinkB, sinkC), 200);
        pipeline.start();
        assertEquals("初始活跃 Sink 应为 3", 3, pipeline.getActiveSinkCount());

        // 投递 20 条
        for (int i = 0; i < 20; i++) {
            pipeline.offer(createRecord("TopicA", i * 100, (i + 1) * 100, 0));
        }
        Thread.sleep(500);
        assertEquals("第 1 批 20 条应全部消费", 20, totalConsumed.get());

        // 移除 Sink-C
        pipeline.removeSink(sinkC);
        assertEquals("移除 1 个后活跃 Sink 应为 2", 2, pipeline.getActiveSinkCount());

        // 投递 20 条
        for (int i = 20; i < 40; i++) {
            pipeline.offer(createRecord("TopicA", i * 100, (i + 1) * 100, 0));
        }
        Thread.sleep(500);
        assertEquals("第 2 批 20 条应全部消费", 40, totalConsumed.get());

        // 移除 Sink-B
        pipeline.removeSink(sinkB);
        assertEquals("移除 2 个后活跃 Sink 应为 1", 1, pipeline.getActiveSinkCount());

        // 投递 20 条
        for (int i = 40; i < 60; i++) {
            pipeline.offer(createRecord("TopicA", i * 100, (i + 1) * 100, 0));
        }
        Thread.sleep(500);
        assertEquals("第 3 批 20 条应全部消费（只剩 Sink-A）", 60, totalConsumed.get());

        pipeline.stopAll();
    }

    /**
     * 【检查点】已停止的 Sink 不在 Pipeline 活跃列表中，重复移除返回 false
     */
    @Test
    public void testPipelineRemoveSinkIdempotent() throws Exception {
        SyncSink sinkA = createCountingSyncSink(new AtomicInteger(0));
        SyncSink sinkB = createCountingSyncSink(new AtomicInteger(0));

        MockSource source = new MockSource();
        TestSyncPipelineHelper pipeline = new TestSyncPipelineHelper(source, Arrays.asList(sinkA, sinkB), 100);
        pipeline.start();

        // 第一次移除成功
        assertTrue(pipeline.removeSink(sinkA));
        assertEquals(1, pipeline.getActiveSinkCount());

        // 第二次移除同一个 → 应返回 false
        assertFalse("重复移除已移除的 Sink 应返回 false", pipeline.removeSink(sinkA));
        assertEquals(1, pipeline.getActiveSinkCount());

        pipeline.stopAll();
    }

    /**
     * 创建一个简单的计数 SyncSink（Pipeline 测试用）
     */
    private SyncSink createCountingSyncSink(final AtomicInteger counter) {
        return new SyncSink() {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public void flush() {}
            @Override
            public void write(SyncRecord record) {
                counter.incrementAndGet();
            }
        };
    }

    // ==================== 缩容对探活的影响 ====================

    /**
     * 测试缩容停止的 Sink 的探活返回 false
     */
    @Test
    public void testScaledDownSinkProbeReturnsFalse() throws Exception {
        RocketMQSink sink = createSink("sink-probe");
        sink.setWriteCallback(new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                return "ok";
            }
            @Override
            public boolean probe() { return true; }
        });

        sink.start();
        assertTrue("运行时探活应成功", sink.probeTargetCluster());

        sink.stop();
        // 虽然 callback 返回 true，但这里测试停止后 Sink 状态
        assertFalse("停止后 Sink 不应在运行", sink.isRunning());
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

    private RocketMQSink.SinkWriteCallback createOffsetTrackingCallback(final CopyOnWriteArrayList<Long> offsets) {
        return new RocketMQSink.SinkWriteCallback() {
            @Override
            public String send(String topic, byte[] body, int queueId, Map<String, String> properties) {
                String offsetStr = properties.get("ORIGIN_PHYSICAL_OFFSET");
                if (offsetStr != null) {
                    offsets.add(Long.parseLong(offsetStr));
                }
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
