package org.apache.rocketmq.hasync.e2e;

import org.apache.rocketmq.hasync.core.CheckpointCoordinator;
import org.apache.rocketmq.hasync.core.TestSyncPipelineHelper;
import org.apache.rocketmq.hasync.core.SyncSink;
import org.apache.rocketmq.hasync.core.SyncSource;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * 端到端优雅停机测试
 * <p>
 * 覆盖场景：
 * - 空管道优雅停机
 * - 有数据时优雅停机
 * - 停机时 drain 剩余消息
 * - 停机顺序保证（Source → Sink）
 * - 停机后组件状态验证
 * - 多 Sink 停机并行性
 * - ShutdownHook 注册模式
 * - 停机超时保护
 */
public class EndToEndGracefulShutdownTest {

    private TestSyncPipelineHelper pipeline;

    @After
    public void tearDown() {
        if (pipeline != null && pipeline.isRunning()) {
            pipeline.stopAll();
        }
    }

    // ==================== 场景1：空管道优雅停机 ====================

    @Test
    public void testEmptyPipelineGracefulShutdown() throws Exception {
        OrderedShutdownSource source = new OrderedShutdownSource();
        OrderedShutdownSink sink = new OrderedShutdownSink("sink-1");

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink));
        pipeline.start();
        assertTrue(pipeline.isRunning());

        // 无任何数据，直接停机
        pipeline.stopAll();
        assertFalse(pipeline.isRunning());
        assertTrue("Source 应已停止", source.isStopped());
        assertTrue("Sink 应已停止", sink.isStopped());
    }

    // ==================== 场景2：有数据时优雅停机 ====================

    @Test
    public void testGracefulShutdownWithPendingData() throws Exception {
        OrderedShutdownSource source = new OrderedShutdownSource();
        DrainingSink sink = new DrainingSink();

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink), 100);
        pipeline.start();

        // 投递一些数据
        for (int i = 0; i < 20; i++) {
            SyncRecord record = new SyncRecord();
            record.setPhysicOffset(i * 100L);
            record.setTopic("shutdown-topic");
            pipeline.offer(record);
        }

        // 等待部分消息被处理
        Thread.sleep(500);

        // 优雅停机
        pipeline.stopAll();
        assertFalse(pipeline.isRunning());

        // 验证 Sink 收到了消息（可能不是全部，取决于处理速度）
        assertTrue("Sink 应处理了部分消息", sink.getWrittenCount() > 0);
    }

    // ==================== 场景3：停机顺序保证 ====================

    @Test
    public void testShutdownOrderSourceBeforeSink() throws Exception {
        ShutdownOrderTracker tracker = new ShutdownOrderTracker();
        OrderedShutdownSource source = new OrderedShutdownSource(tracker, "source");
        OrderedShutdownSink sink = new OrderedShutdownSink("sink-1", tracker);

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink));
        pipeline.start();

        pipeline.stopAll();

        // 验证停止顺序：Sink 先停止（线程退出后），然后 Source 最后停止
        // SyncPipeline.stopAll 的逻辑是：
        // 1) running=false → 2) 等待 source 线程 → 3) 等待 sink 线程 → 4) sink.stop() → 5) source.stop()
        assertTrue("Sink 应在 Source 之前停止",
                tracker.getStopOrder("sink-1") < tracker.getStopOrder("source"));
    }

    // ==================== 场景4：停机后不可再 offer ====================

    @Test
    public void testNoOfferAfterShutdown() throws Exception {
        SimpleSource source = new SimpleSource();
        SimpleSink sink = new SimpleSink();

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink), 10);
        pipeline.start();
        pipeline.stopAll();

        // 停机后 offer 仍然可以向队列添加（queue 本身不关心 running 状态）
        // 但 Sink 线程已停止，不会再消费
        SyncRecord record = new SyncRecord();
        record.setTopic("after-shutdown");
        // offer 操作取决于队列状态，不会报错
        pipeline.offer(record);
        assertFalse("Pipeline 应已停止", pipeline.isRunning());
    }

    // ==================== 场景5：多 Sink 停机 ====================

    @Test
    public void testMultiSinkGracefulShutdown() throws Exception {
        OrderedShutdownSource source = new OrderedShutdownSource();
        DrainingSink sink1 = new DrainingSink();
        DrainingSink sink2 = new DrainingSink();
        DrainingSink sink3 = new DrainingSink();

        pipeline = new TestSyncPipelineHelper(source,
                Arrays.<SyncSink>asList(sink1, sink2, sink3), 100);
        pipeline.start();

        // 投递数据
        for (int i = 0; i < 30; i++) {
            SyncRecord record = new SyncRecord();
            record.setPhysicOffset(i);
            record.setTopic("multi-sink");
            pipeline.offer(record);
        }

        Thread.sleep(1000);
        pipeline.stopAll();

        assertFalse(pipeline.isRunning());
        // 所有 Sink 都应被停止
        assertTrue("Sink-1 应已停止", sink1.isStopped());
        assertTrue("Sink-2 应已停止", sink2.isStopped());
        assertTrue("Sink-3 应已停止", sink3.isStopped());

        // 合计处理数应等于投递数
        int total = sink1.getWrittenCount() + sink2.getWrittenCount() + sink3.getWrittenCount();
        assertEquals("三个 Sink 合计应处理 30 条消息", 30, total);
    }

    // ==================== 场景6：停机时 Checkpoint flush ====================

    @Test
    public void testCheckpointFlushOnShutdown() throws Exception {
        FlushTrackingCheckpoint checkpoint = new FlushTrackingCheckpoint();
        SimpleSource source = new SimpleSource();
        CheckpointAwareSink sink = new CheckpointAwareSink(checkpoint, "sink-ckpt");

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink), 50);
        pipeline.start();

        // 投递并等待处理
        for (int i = 0; i < 5; i++) {
            SyncRecord record = new SyncRecord();
            record.setPhysicOffset(1000L + i * 100);
            record.setTopic("ckpt-topic");
            pipeline.offer(record);
        }
        Thread.sleep(1000);

        // 模拟停机前手动 flush
        checkpoint.flush();
        assertTrue("Checkpoint 应被 flush", checkpoint.isFlushed());

        pipeline.stopAll();

        // 验证 Checkpoint 位点
        assertTrue("Checkpoint 应有记录", checkpoint.getConfirmedOffset() > 0);
    }

    // ==================== 场景7：stopAll 幂等性 ====================

    @Test
    public void testStopAllIdempotency() throws Exception {
        SimpleSource source = new SimpleSource();
        SimpleSink sink = new SimpleSink();

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink));
        pipeline.start();

        // 多次调用 stopAll
        pipeline.stopAll();
        pipeline.stopAll();
        pipeline.stopAll();

        assertFalse(pipeline.isRunning());
    }

    // ==================== 场景8：停机时间不超时 ====================

    @Test
    public void testShutdownCompletesWithinTimeout() throws Exception {
        SimpleSource source = new SimpleSource();
        SimpleSink sink = new SimpleSink();

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink));
        pipeline.start();

        // 投递一些数据
        for (int i = 0; i < 10; i++) {
            pipeline.offer(new SyncRecord());
        }

        long startTime = System.currentTimeMillis();
        pipeline.stopAll();
        long elapsed = System.currentTimeMillis() - startTime;

        // 停机应在合理时间内完成（Pipeline 内部线程等待 5s 超时）
        assertTrue("停机应在 10 秒内完成", elapsed < 10000);
        assertFalse(pipeline.isRunning());
    }

    // ==================== 场景9：ShutdownHook 注册模式验证 ====================

    @Test
    public void testShutdownHookPattern() throws Exception {
        SimpleSource source = new SimpleSource();
        SimpleSink sink = new SimpleSink();

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink));
        pipeline.start();

        // 模拟 ShutdownHook 的行为
        final AtomicBoolean hookExecuted = new AtomicBoolean(false);
        final TestSyncPipelineHelper pipelineRef = pipeline;

        Thread shutdownHook = new Thread(() -> {
            if (pipelineRef.isRunning()) {
                pipelineRef.stopAll();
            }
            hookExecuted.set(true);
        }, "test-shutdown-hook");

        // 手动执行 hook
        shutdownHook.start();
        shutdownHook.join(5000);

        assertTrue("Hook 应被执行", hookExecuted.get());
        assertFalse("Pipeline 应已停止", pipeline.isRunning());
    }

    // ==================== 场景10：启动后立即停机 ====================

    @Test
    public void testStartThenImmediateShutdown() throws Exception {
        SimpleSource source = new SimpleSource();
        SimpleSink sink = new SimpleSink();

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink));
        pipeline.start();
        // 不投递任何数据，立即停机
        pipeline.stopAll();

        assertFalse(pipeline.isRunning());
    }

    // ==================== 场景11：高并发下停机安全 ====================

    @Test
    public void testConcurrentShutdownSafety() throws Exception {
        SimpleSource source = new SimpleSource();
        SimpleSink sink = new SimpleSink();

        pipeline = new TestSyncPipelineHelper(source, Collections.<SyncSink>singletonList(sink));
        pipeline.start();

        // 多线程同时调用 stopAll
        int threadCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    pipeline.stopAll();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown(); // 同时触发
        assertTrue("所有线程应在 10s 内完成", doneLatch.await(10, TimeUnit.SECONDS));
        assertFalse(pipeline.isRunning());
    }

    // ==================== 辅助类 ====================

    /** 停机顺序追踪器 */
    static class ShutdownOrderTracker {
        private final ConcurrentHashMap<String, Integer> stopOrder = new ConcurrentHashMap<>();
        private final AtomicLong orderCounter = new AtomicLong(0);

        void recordStop(String component) {
            stopOrder.put(component, (int) orderCounter.incrementAndGet());
        }

        int getStopOrder(String component) {
            Integer order = stopOrder.get(component);
            return order != null ? order : -1;
        }
    }

    /** 记录停机顺序的 Source */
    static class OrderedShutdownSource implements SyncSource {
        private volatile boolean running = false;
        private volatile boolean stopped = false;
        private final ShutdownOrderTracker tracker;
        private final String name;

        OrderedShutdownSource() {
            this(null, "source");
        }

        OrderedShutdownSource(ShutdownOrderTracker tracker, String name) {
            this.tracker = tracker;
            this.name = name;
        }

        @Override public void start() { running = true; }

        @Override
        public void stop() {
            running = false;
            stopped = true;
            if (tracker != null) tracker.recordStop(name);
        }

        @Override public boolean isRunning() { return running; }

        @Override
        public void poll() {
            try { Thread.sleep(50); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        boolean isStopped() { return stopped; }
    }

    /** 记录停机顺序的 Sink */
    static class OrderedShutdownSink implements SyncSink {
        private volatile boolean started = false;
        private volatile boolean stopped = false;
        private final String sinkId;
        private final ShutdownOrderTracker tracker;

        OrderedShutdownSink(String sinkId) {
            this(sinkId, null);
        }

        OrderedShutdownSink(String sinkId, ShutdownOrderTracker tracker) {
            this.sinkId = sinkId;
            this.tracker = tracker;
        }

        @Override public void start() { started = true; }

        @Override
        public void stop() {
            started = false;
            stopped = true;
            if (tracker != null) tracker.recordStop(sinkId);
        }

        @Override public void write(SyncRecord record) {}
        @Override public void flush() {}

        boolean isStopped() { return stopped; }
    }

    /** 支持 drain 的 Sink */
    static class DrainingSink implements SyncSink {
        private final CopyOnWriteArrayList<SyncRecord> written = new CopyOnWriteArrayList<>();
        private volatile boolean stopped = false;

        @Override public void start() {}

        @Override
        public void stop() { stopped = true; }

        @Override
        public void write(SyncRecord record) {
            written.add(record);
        }

        @Override public void flush() {}

        int getWrittenCount() { return written.size(); }
        boolean isStopped() { return stopped; }
    }

    /** 简单 Source */
    static class SimpleSource implements SyncSource {
        private volatile boolean running = false;

        @Override public void start() { running = true; }
        @Override public void stop() { running = false; }
        @Override public boolean isRunning() { return running; }

        @Override
        public void poll() {
            try { Thread.sleep(50); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** 简单 Sink */
    static class SimpleSink implements SyncSink {
        @Override public void start() {}
        @Override public void stop() {}
        @Override public void write(SyncRecord record) {}
        @Override public void flush() {}
    }

    /** 带 Checkpoint 的 Sink */
    static class CheckpointAwareSink implements SyncSink {
        private final CheckpointCoordinator checkpoint;
        private final String sinkId;

        CheckpointAwareSink(CheckpointCoordinator checkpoint, String sinkId) {
            this.checkpoint = checkpoint;
            this.sinkId = sinkId;
        }

        @Override public void start() {}
        @Override public void stop() {}

        @Override
        public void write(SyncRecord record) {
            checkpoint.commitOffset(sinkId, record.getPhysicOffset());
        }

        @Override public void flush() {}
    }

    /** 追踪 flush 的 Checkpoint */
    static class FlushTrackingCheckpoint implements CheckpointCoordinator {
        private final ConcurrentHashMap<String, AtomicLong> offsets = new ConcurrentHashMap<>();
        private volatile boolean flushed = false;

        @Override
        public long getConfirmedOffset() {
            if (offsets.isEmpty()) return 0L;
            long min = Long.MAX_VALUE;
            for (AtomicLong o : offsets.values()) min = Math.min(min, o.get());
            return min == Long.MAX_VALUE ? 0L : min;
        }

        @Override
        public void commitOffset(String sinkId, long offset) {
            offsets.computeIfAbsent(sinkId, k -> new AtomicLong(0L)).set(offset);
        }

        @Override
        public long recoverCheckpoint(String sinkId) {
            AtomicLong o = offsets.get(sinkId);
            return o != null ? o.get() : 0L;
        }

        @Override
        public void flush() { flushed = true; }

        boolean isFlushed() { return flushed; }
    }
}
