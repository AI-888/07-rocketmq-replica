package org.apache.rocketmq.hasync.e2e;

import org.apache.rocketmq.hasync.core.CheckpointCoordinator;
import org.apache.rocketmq.hasync.core.SyncPipeline;
import org.apache.rocketmq.hasync.core.SyncSink;
import org.apache.rocketmq.hasync.core.SyncSource;
import org.apache.rocketmq.hasync.model.PullRequest;
import org.apache.rocketmq.hasync.model.PullResponse;
import org.apache.rocketmq.hasync.model.ResponseStatus;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * 端到端数据流测试
 * <p>
 * 模拟完整的数据同步链路：
 * Source 产生数据 → Pipeline 缓冲 → Sink 写入 → Checkpoint 位点管理
 * <p>
 * 覆盖场景：
 * - 单条消息端到端
 * - 批量消息顺序保证
 * - 多 Topic 混合
 * - 多 Sink 并发消费
 * - Checkpoint 位点正确推进
 * - PullRequest/PullResponse 完整交互
 */
public class EndToEndDataFlowTest {

    private SyncPipeline pipeline;

    @After
    public void tearDown() {
        if (pipeline != null && pipeline.isRunning()) {
            pipeline.stopAll();
        }
    }

    // ==================== 场景1：单条消息端到端 ====================

    @Test
    public void testSingleMessageEndToEnd() throws Exception {
        // 准备数据
        SyncRecord record = createRecord(1000L, "test-topic", 0, "hello-world");

        // 创建带 Checkpoint 的 Sink
        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
        RecordingSink sink = new RecordingSink(checkpoint, "sink-1");
        SimulatedSource source = new SimulatedSource(Collections.singletonList(record));

        // 组装并启动 Pipeline
        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 100);
        pipeline.start();

        // Source 投递数据到 Pipeline
        assertTrue(pipeline.offer(record));

        // 等待 Sink 处理
        assertTrue("Sink 应在超时前收到消息", sink.awaitRecords(1, 5, TimeUnit.SECONDS));

        // 验证
        assertEquals(1, sink.getWrittenRecords().size());
        SyncRecord written = sink.getWrittenRecords().get(0);
        assertEquals(1000L, written.getPhysicOffset());
        assertEquals("test-topic", written.getTopic());
        assertEquals(0, written.getQueueId());
        assertArrayEquals("hello-world".getBytes(), written.getBody());

        // 验证 Checkpoint 已推进
        assertEquals(1000L, checkpoint.getConfirmedOffset());
    }

    // ==================== 场景2：批量消息顺序保证 ====================

    @Test
    public void testBatchMessageOrderPreservation() throws Exception {
        // 构造 100 条有序消息
        List<SyncRecord> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            records.add(createRecord(1000L + i * 100, "order-topic", i % 4, "msg-" + i));
        }

        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
        OrderVerifyingSink sink = new OrderVerifyingSink(checkpoint, "sink-order");
        SimulatedSource source = new SimulatedSource(records);

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 200);
        pipeline.start();

        // 按顺序投递所有消息
        for (SyncRecord record : records) {
            while (!pipeline.offer(record, 100, TimeUnit.MILLISECONDS)) {
                // 队列满时等待
            }
        }

        // 等待 Sink 处理完所有消息
        assertTrue("Sink 应处理完全部 100 条消息",
                sink.awaitRecords(100, 10, TimeUnit.SECONDS));

        // 验证顺序 — Sink 中消息的 physicOffset 应该单调递增
        assertTrue("消息顺序应被保持", sink.isOrderPreserved());
        assertEquals(100, sink.getWrittenRecords().size());

        // 验证最终 Checkpoint
        long lastOffset = records.get(records.size() - 1).getPhysicOffset();
        assertEquals(lastOffset, checkpoint.getConfirmedOffset());
    }

    // ==================== 场景3：多 Topic 混合数据流 ====================

    @Test
    public void testMultiTopicDataFlow() throws Exception {
        // 构造多 Topic 混合消息
        List<SyncRecord> records = new ArrayList<>();
        String[] topics = {"topic-A", "topic-B", "topic-C"};
        for (int i = 0; i < 30; i++) {
            records.add(createRecord(2000L + i * 50, topics[i % 3], i % 8, "multi-" + i));
        }

        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
        TopicGroupingSink sink = new TopicGroupingSink(checkpoint, "sink-multi");
        SimulatedSource source = new SimulatedSource(records);

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 100);
        pipeline.start();

        for (SyncRecord record : records) {
            assertTrue(pipeline.offer(record, 1, TimeUnit.SECONDS));
        }

        assertTrue("Sink 应处理完全部 30 条消息",
                sink.awaitRecords(30, 10, TimeUnit.SECONDS));

        // 验证按 Topic 分组
        Map<String, List<SyncRecord>> grouped = sink.getGroupedByTopic();
        assertEquals(3, grouped.size());
        assertEquals(10, grouped.get("topic-A").size());
        assertEquals(10, grouped.get("topic-B").size());
        assertEquals(10, grouped.get("topic-C").size());

        // 每个 Topic 内的消息顺序也应正确（physicOffset 递增）
        for (Map.Entry<String, List<SyncRecord>> entry : grouped.entrySet()) {
            List<SyncRecord> topicRecords = entry.getValue();
            for (int i = 1; i < topicRecords.size(); i++) {
                assertTrue(entry.getKey() + " 内部顺序应递增",
                        topicRecords.get(i).getPhysicOffset() > topicRecords.get(i - 1).getPhysicOffset());
            }
        }
    }

    // ==================== 场景4：多 Sink 并发消费 ====================

    @Test
    public void testMultiSinkConcurrentConsumption() throws Exception {
        // 准备数据
        List<SyncRecord> records = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            records.add(createRecord(3000L + i * 10, "concurrent-topic", 0, "c-" + i));
        }

        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
        RecordingSink sink1 = new RecordingSink(checkpoint, "sink-1");
        RecordingSink sink2 = new RecordingSink(checkpoint, "sink-2");
        SimulatedSource source = new SimulatedSource(records);

        pipeline = new SyncPipeline(source, Arrays.<SyncSink>asList(sink1, sink2), 100);
        pipeline.start();

        for (SyncRecord record : records) {
            assertTrue(pipeline.offer(record, 1, TimeUnit.SECONDS));
        }

        // 等待两个 Sink 合计处理完所有消息
        // 注意：因为两个 Sink 竞争同一个 queue，所以消息会被分散消费
        Thread.sleep(3000); // 给足够时间让两个 Sink 处理

        int totalProcessed = sink1.getWrittenRecords().size() + sink2.getWrittenRecords().size();
        assertEquals("两个 Sink 合计应处理 50 条消息", 50, totalProcessed);

        // 两个 Sink 在竞争消费时，不能保证每个 Sink 都一定能拿到消息
        // 只验证合计数正确即可
        assertTrue("至少有一个 Sink 参与了消费",
                sink1.getWrittenRecords().size() > 0 || sink2.getWrittenRecords().size() > 0);
    }

    // ==================== 场景5：PullRequest/PullResponse 完整交互 ====================

    @Test
    public void testPullRequestResponseInteraction() {
        // 模拟 Source 缓冲区中的数据
        List<SyncRecord> bufferedRecords = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            bufferedRecords.add(createRecord(5000L + i * 100, "pull-topic", 0, "pull-" + i));
        }

        // 模拟 Sink 发起 PullRequest
        PullRequest request = new PullRequest(5000L, 10, "sink-pull-1");
        assertNotNull(request.getSinkId());
        assertEquals(5000L, request.getFromOffset());
        assertEquals(10, request.getBatchSize());

        // 模拟 Source 处理请求并返回 PullResponse
        List<SyncRecord> responseRecords = new ArrayList<>();
        for (SyncRecord record : bufferedRecords) {
            if (record.getPhysicOffset() >= request.getFromOffset()
                    && responseRecords.size() < request.getBatchSize()) {
                responseRecords.add(record);
            }
        }

        PullResponse response = new PullResponse(responseRecords,
                bufferedRecords.get(bufferedRecords.size() - 1).getPhysicOffset());
        response.setStatus(ResponseStatus.SUCCESS);

        // 验证 PullResponse
        assertEquals(ResponseStatus.SUCCESS, response.getStatus());
        assertEquals(10, response.getRecords().size());
        assertEquals(5000L, response.getRecords().get(0).getPhysicOffset());
        assertEquals(5900L, response.getRecords().get(9).getPhysicOffset());
        assertEquals(6900L, response.getMaxOffset());

        // 模拟第二次请求（从上次结束位置继续）
        PullRequest request2 = new PullRequest(5900L + 100, 10, "sink-pull-1");
        List<SyncRecord> responseRecords2 = new ArrayList<>();
        for (SyncRecord record : bufferedRecords) {
            if (record.getPhysicOffset() >= request2.getFromOffset()
                    && responseRecords2.size() < request2.getBatchSize()) {
                responseRecords2.add(record);
            }
        }

        PullResponse response2 = new PullResponse(responseRecords2, 6900L);
        assertEquals(10, response2.getRecords().size());
        assertEquals(6000L, response2.getRecords().get(0).getPhysicOffset());
    }

    // ==================== 场景6：PullResponse NO_NEW_DATA 状态 ====================

    @Test
    public void testPullResponseNoNewData() {
        PullResponse response = new PullResponse(ResponseStatus.NO_NEW_MSG);
        assertEquals(ResponseStatus.NO_NEW_MSG, response.getStatus());
        assertNull(response.getRecords());
    }

    // ==================== 场景7：PullResponse OFFSET_TOO_SMALL 状态 ====================

    @Test
    public void testPullResponseOffsetTooSmall() {
        PullResponse response = new PullResponse(ResponseStatus.OFFSET_ILLEGAL);
        assertEquals(ResponseStatus.OFFSET_ILLEGAL, response.getStatus());
    }

    // ==================== 场景8：Checkpoint 分步推进 ====================

    @Test
    public void testCheckpointProgressiveCommit() {
        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();

        // 模拟 Sink-1 和 Sink-2 的位点推进
        checkpoint.commitOffset("sink-1", 1000L);
        checkpoint.commitOffset("sink-2", 500L);

        // globalCheckpoint = min(1000, 500) = 500
        assertEquals(500L, checkpoint.getConfirmedOffset());

        // Sink-2 追上
        checkpoint.commitOffset("sink-2", 1000L);
        assertEquals(1000L, checkpoint.getConfirmedOffset());

        // Sink-1 继续前进
        checkpoint.commitOffset("sink-1", 2000L);
        // globalCheckpoint = min(2000, 1000) = 1000
        assertEquals(1000L, checkpoint.getConfirmedOffset());

        // 两个 Sink 都前进
        checkpoint.commitOffset("sink-2", 2000L);
        assertEquals(2000L, checkpoint.getConfirmedOffset());
    }

    // ==================== 场景9：Checkpoint 恢复 ====================

    @Test
    public void testCheckpointRecovery() {
        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();

        // 首次启动 — 无历史位点
        assertEquals(0L, checkpoint.recoverCheckpoint("new-sink"));

        // 提交一些位点
        checkpoint.commitOffset("existing-sink", 5000L);

        // 模拟重启恢复
        assertEquals(5000L, checkpoint.recoverCheckpoint("existing-sink"));
    }

    // ==================== 场景10：Checkpoint flush ====================

    @Test
    public void testCheckpointFlush() {
        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
        checkpoint.commitOffset("sink-1", 8000L);
        checkpoint.commitOffset("sink-2", 7500L);

        // 强制 flush
        checkpoint.flush();

        assertTrue("flush 应被调用", checkpoint.isFlushed());
        // flush 后位点应保持不变
        assertEquals(7500L, checkpoint.getConfirmedOffset());
    }

    // ==================== 场景11：SyncRecord 属性透传 ====================

    @Test
    public void testSyncRecordPropertyPreservation() throws Exception {
        SyncRecord record = createRecord(9000L, "prop-topic", 2, "prop-body");
        record.setTraceId("node1-9000-0");
        record.putProperty("ORIGIN_PHYSICAL_OFFSET", "9000");
        record.putProperty("SYNC_TRACE_ID", "node1-9000-0");
        record.setStoreTimestamp(System.currentTimeMillis() - 1000);
        record.setReceiveTimestamp(System.currentTimeMillis());
        record.setMasterPhyOffset(8000L);
        record.setEndOffset(9200L);
        record.setMsgSize(200);

        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
        RecordingSink sink = new RecordingSink(checkpoint, "sink-prop");
        SimulatedSource source = new SimulatedSource(Collections.singletonList(record));

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 10);
        pipeline.start();
        pipeline.offer(record);

        assertTrue(sink.awaitRecords(1, 5, TimeUnit.SECONDS));

        SyncRecord written = sink.getWrittenRecords().get(0);
        // 验证所有属性完整透传
        assertEquals("node1-9000-0", written.getTraceId());
        assertEquals("9000", written.getProperties().get("ORIGIN_PHYSICAL_OFFSET"));
        assertEquals("node1-9000-0", written.getProperties().get("SYNC_TRACE_ID"));
        assertEquals(9000L, written.getPhysicOffset());
        assertEquals(2, written.getQueueId());
        assertEquals(8000L, written.getMasterPhyOffset());
        assertEquals(9200L, written.getEndOffset());
        assertEquals(200, written.getMsgSize());
        assertTrue(written.getStoreTimestamp() > 0);
        assertTrue(written.getReceiveTimestamp() > 0);
    }

    // ==================== 场景12：大批量数据端到端 ====================

    @Test
    public void testLargeBatchEndToEnd() throws Exception {
        int totalMessages = 1000;
        List<SyncRecord> records = new ArrayList<>();
        for (int i = 0; i < totalMessages; i++) {
            records.add(createRecord(10000L + i, "large-batch-topic", i % 16, "lb-" + i));
        }

        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
        RecordingSink sink = new RecordingSink(checkpoint, "sink-large");
        SimulatedSource source = new SimulatedSource(records);

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 500);
        pipeline.start();

        // 使用独立线程投递，模拟 Source 持续产出
        Thread producer = new Thread(() -> {
            for (SyncRecord record : records) {
                try {
                    while (!pipeline.offer(record, 200, TimeUnit.MILLISECONDS)) {
                        if (!pipeline.isRunning()) break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        producer.start();

        // 等待所有消息被处理
        assertTrue("Sink 应在超时前处理完 1000 条消息",
                sink.awaitRecords(totalMessages, 30, TimeUnit.SECONDS));

        producer.join(5000);

        assertEquals(totalMessages, sink.getWrittenRecords().size());
        assertEquals(10000L + totalMessages - 1, checkpoint.getConfirmedOffset());
    }

    // ==================== 场景13：SyncRecord 的 TopicFilter 交互 ====================

    @Test
    public void testPullRequestWithTopicFilter() {
        PullRequest request = new PullRequest(0, 50, "sink-filter");
        request.setTopicFilter(new java.util.HashSet<>(Arrays.asList("topicA", "topicB")));

        // 验证 TopicFilter 设置
        assertNotNull(request.getTopicFilter());
        assertEquals(2, request.getTopicFilter().size());
        assertTrue(request.getTopicFilter().contains("topicA"));
        assertTrue(request.getTopicFilter().contains("topicB"));

        // 模拟 Source 根据 TopicFilter 过滤
        List<SyncRecord> allRecords = new ArrayList<>();
        allRecords.add(createRecord(100, "topicA", 0, "a"));
        allRecords.add(createRecord(200, "topicC", 0, "c")); // 应被过滤
        allRecords.add(createRecord(300, "topicB", 0, "b"));

        List<SyncRecord> filtered = new ArrayList<>();
        for (SyncRecord record : allRecords) {
            if (request.getTopicFilter() == null || request.getTopicFilter().contains(record.getTopic())) {
                filtered.add(record);
            }
        }

        assertEquals(2, filtered.size());
        assertEquals("topicA", filtered.get(0).getTopic());
        assertEquals("topicB", filtered.get(1).getTopic());
    }

    // ==================== 场景14：Pipeline 队列容量边界 ====================

    @Test
    public void testPipelineQueueCapacityBoundary() throws Exception {
        int capacity = 5;
        RecordingSink sink = new RecordingSink(new InMemoryCheckpoint(), "sink-boundary");
        SimulatedSource source = new SimulatedSource(Collections.<SyncRecord>emptyList());

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), capacity);
        // 未启动时投递（只放入队列，无消费者）
        for (int i = 0; i < capacity; i++) {
            assertTrue("第 " + (i + 1) + " 条应成功投递",
                    pipeline.offer(createRecord(i, "cap-topic", 0, "cap-" + i)));
        }
        assertEquals(capacity, pipeline.getQueueSize());
        assertEquals(0, pipeline.getQueueRemainingCapacity());

        // 超过容量应失败
        assertFalse(pipeline.offer(createRecord(999, "cap-topic", 0, "overflow")));

        // 启动后 Sink 开始消费
        pipeline.start();
        assertTrue(sink.awaitRecords(capacity, 5, TimeUnit.SECONDS));
        assertEquals(capacity, sink.getWrittenRecords().size());
    }

    // ==================== 辅助方法 ====================

    private SyncRecord createRecord(long offset, String topic, int queueId, String body) {
        SyncRecord record = new SyncRecord();
        record.setPhysicOffset(offset);
        record.setTopic(topic);
        record.setQueueId(queueId);
        record.setBody(body.getBytes());
        record.setMsgSize(body.getBytes().length);
        record.setMasterPhyOffset(offset - 100);
        record.setEndOffset(offset + body.getBytes().length);
        record.setStoreTimestamp(System.currentTimeMillis());
        record.setReceiveTimestamp(System.currentTimeMillis());
        record.setTraceId("test-" + offset + "-0");
        return record;
    }

    // ==================== 内部模拟实现 ====================

    /**
     * 模拟 SyncSource — 持有预设数据列表
     */
    static class SimulatedSource implements SyncSource {
        private final List<SyncRecord> records;
        private volatile boolean running = false;

        SimulatedSource(List<SyncRecord> records) {
            this.records = new ArrayList<>(records);
        }

        @Override
        public void start() {
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public void poll() {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 带记录功能的 Sink — 记录所有写入的 SyncRecord
     */
    static class RecordingSink implements SyncSink {
        private final CopyOnWriteArrayList<SyncRecord> writtenRecords = new CopyOnWriteArrayList<>();
        private final CheckpointCoordinator checkpoint;
        private final String sinkId;
        private volatile boolean started = false;
        private volatile CountDownLatch latch;

        RecordingSink(CheckpointCoordinator checkpoint, String sinkId) {
            this.checkpoint = checkpoint;
            this.sinkId = sinkId;
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void stop() {
            started = false;
        }

        @Override
        public void write(SyncRecord record) {
            writtenRecords.add(record);
            checkpoint.commitOffset(sinkId, record.getPhysicOffset());
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public void flush() {
        }

        boolean awaitRecords(int count, long timeout, TimeUnit unit) throws InterruptedException {
            latch = new CountDownLatch(count);
            // 已有的记录也计入
            for (int i = 0; i < writtenRecords.size() && latch.getCount() > 0; i++) {
                latch.countDown();
            }
            return latch.await(timeout, unit);
        }

        List<SyncRecord> getWrittenRecords() {
            return writtenRecords;
        }
    }

    /**
     * 验证写入顺序的 Sink
     */
    static class OrderVerifyingSink implements SyncSink {
        private final CopyOnWriteArrayList<SyncRecord> writtenRecords = new CopyOnWriteArrayList<>();
        private final CheckpointCoordinator checkpoint;
        private final String sinkId;
        private volatile boolean started = false;
        private volatile CountDownLatch latch;

        OrderVerifyingSink(CheckpointCoordinator checkpoint, String sinkId) {
            this.checkpoint = checkpoint;
            this.sinkId = sinkId;
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void stop() {
            started = false;
        }

        @Override
        public void write(SyncRecord record) {
            writtenRecords.add(record);
            checkpoint.commitOffset(sinkId, record.getPhysicOffset());
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public void flush() {
        }

        boolean awaitRecords(int count, long timeout, TimeUnit unit) throws InterruptedException {
            latch = new CountDownLatch(count);
            for (int i = 0; i < writtenRecords.size() && latch.getCount() > 0; i++) {
                latch.countDown();
            }
            return latch.await(timeout, unit);
        }

        boolean isOrderPreserved() {
            for (int i = 1; i < writtenRecords.size(); i++) {
                if (writtenRecords.get(i).getPhysicOffset() <= writtenRecords.get(i - 1).getPhysicOffset()) {
                    return false;
                }
            }
            return true;
        }

        List<SyncRecord> getWrittenRecords() {
            return writtenRecords;
        }
    }

    /**
     * 按 Topic 分组的 Sink
     */
    static class TopicGroupingSink implements SyncSink {
        private final CopyOnWriteArrayList<SyncRecord> writtenRecords = new CopyOnWriteArrayList<>();
        private final CheckpointCoordinator checkpoint;
        private final String sinkId;
        private volatile boolean started = false;
        private volatile CountDownLatch latch;

        TopicGroupingSink(CheckpointCoordinator checkpoint, String sinkId) {
            this.checkpoint = checkpoint;
            this.sinkId = sinkId;
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void stop() {
            started = false;
        }

        @Override
        public void write(SyncRecord record) {
            writtenRecords.add(record);
            checkpoint.commitOffset(sinkId, record.getPhysicOffset());
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public void flush() {
        }

        boolean awaitRecords(int count, long timeout, TimeUnit unit) throws InterruptedException {
            latch = new CountDownLatch(count);
            for (int i = 0; i < writtenRecords.size() && latch.getCount() > 0; i++) {
                latch.countDown();
            }
            return latch.await(timeout, unit);
        }

        Map<String, List<SyncRecord>> getGroupedByTopic() {
            Map<String, List<SyncRecord>> grouped = new HashMap<>();
            for (SyncRecord record : writtenRecords) {
                grouped.computeIfAbsent(record.getTopic(), k -> new ArrayList<>()).add(record);
            }
            return grouped;
        }

        List<SyncRecord> getWrittenRecords() {
            return writtenRecords;
        }
    }

    /**
     * 内存 Checkpoint 实现 — 模拟 NameServer KV 存储
     */
    static class InMemoryCheckpoint implements CheckpointCoordinator {
        private final ConcurrentHashMap<String, AtomicLong> offsets = new ConcurrentHashMap<>();
        private volatile boolean flushed = false;

        @Override
        public long getConfirmedOffset() {
            if (offsets.isEmpty()) {
                return 0L;
            }
            long min = Long.MAX_VALUE;
            for (AtomicLong offset : offsets.values()) {
                min = Math.min(min, offset.get());
            }
            return min == Long.MAX_VALUE ? 0L : min;
        }

        @Override
        public void commitOffset(String sinkId, long offset) {
            offsets.computeIfAbsent(sinkId, k -> new AtomicLong(0L)).set(offset);
        }

        @Override
        public long recoverCheckpoint(String sinkId) {
            AtomicLong offset = offsets.get(sinkId);
            return offset != null ? offset.get() : 0L;
        }

        @Override
        public void flush() {
            flushed = true;
        }

        boolean isFlushed() {
            return flushed;
        }
    }
}
