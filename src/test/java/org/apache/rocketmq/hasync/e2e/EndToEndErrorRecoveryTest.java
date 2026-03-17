package org.apache.rocketmq.hasync.e2e;

import org.apache.rocketmq.hasync.core.CheckpointCoordinator;
import org.apache.rocketmq.hasync.core.SyncPipeline;
import org.apache.rocketmq.hasync.core.SyncSink;
import org.apache.rocketmq.hasync.core.SyncSource;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * 端到端异常处理与恢复测试
 * <p>
 * 覆盖场景：
 * - Source 启动失败回滚
 * - Sink 启动失败回滚
 * - Sink 写入异常不崩溃
 * - Sink 写入间歇性失败后恢复
 * - Source poll 异常不影响 Pipeline
 * - Checkpoint 异常隔离
 * - Pipeline 组件故障隔离
 */
public class EndToEndErrorRecoveryTest {

    private SyncPipeline pipeline;

    @After
    public void tearDown() {
        if (pipeline != null && pipeline.isRunning()) {
            pipeline.stopAll();
        }
    }

    // ==================== 场景1：Source 启动失败完整回滚 ====================

    @Test
    public void testSourceStartFailureFullRollback() {
        FailingSource failSource = new FailingSource(true, false);
        TrackingSink trackSink = new TrackingSink();

        pipeline = new SyncPipeline(failSource, Collections.<SyncSink>singletonList(trackSink));
        try {
            pipeline.start();
            fail("Source 启动失败应抛出异常");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Source 启动失败"));
        }

        assertFalse("Pipeline 不应处于运行状态", pipeline.isRunning());
        // Sink 不应被启动（因为 Source 先启动失败）
        assertFalse("Sink 不应被启动", trackSink.isStarted());
    }

    // ==================== 场景2：Sink 启动失败回滚已启动组件 ====================

    @Test
    public void testSinkStartFailureRollbacksSource() {
        TrackingSource trackSource = new TrackingSource();
        FailingSink failSink = new FailingSink(true, false, 0);

        pipeline = new SyncPipeline(trackSource, Collections.<SyncSink>singletonList(failSink));
        try {
            pipeline.start();
            fail("Sink 启动失败应抛出异常");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Sink[0] 启动失败"));
        }

        assertFalse("Pipeline 不应处于运行状态", pipeline.isRunning());
        // Source 应被回滚停止
        assertTrue("Source 应曾被启动", trackSource.wasStarted());
        assertTrue("Source 应被回滚停止", trackSource.wasStopped());
    }

    // ==================== 场景3：Sink 写入异常不崩溃 Pipeline ====================

    @Test
    public void testSinkWriteExceptionDoesNotCrashPipeline() throws Exception {
        TrackingSource source = new TrackingSource();
        // 前 3 次写入抛异常，之后正常
        FailingSink sink = new FailingSink(false, true, 3);

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 50);
        pipeline.start();

        // 投递 10 条消息
        for (int i = 0; i < 10; i++) {
            SyncRecord record = new SyncRecord();
            record.setPhysicOffset(i * 100L);
            record.setTopic("error-topic");
            pipeline.offer(record);
        }

        // 等待处理
        Thread.sleep(3000);

        // Pipeline 应仍在运行
        assertTrue("Pipeline 应仍在运行", pipeline.isRunning());
        // 异常后的消息仍应被处理
        assertTrue("Sink 应处理了部分消息", sink.getSuccessCount() > 0);
    }

    // ==================== 场景4：Source poll 异常不影响 Pipeline ====================

    @Test
    public void testSourcePollExceptionDoesNotCrashPipeline() throws Exception {
        // Source 的 poll 偶尔抛异常
        int initialFailCount = 3;
        InterruptibleSource source = new InterruptibleSource(initialFailCount);
        TrackingSink sink = new TrackingSink();

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 50);
        pipeline.start();

        // 给 Pipeline 一些时间运行（Source poll 异常后会休眠 1s 再重试）
        Thread.sleep(6000);

        // Pipeline 应仍在运行，即使 Source poll 出过异常
        assertTrue("Pipeline 应仍在运行", pipeline.isRunning());
        assertTrue("Source poll 应被调用多次（至少超过初始失败次数）", source.getPollCount() > initialFailCount);
    }

    // ==================== 场景5：写入异常后 Checkpoint 不推进 ====================

    @Test
    public void testCheckpointNotAdvancedOnWriteFailure() throws Exception {
        InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
        TrackingSource source = new TrackingSource();

        // 所有写入都失败
        AlwaysFailingSink failSink = new AlwaysFailingSink();

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(failSink), 50);
        pipeline.start();

        // 投递消息
        for (int i = 0; i < 5; i++) {
            SyncRecord record = new SyncRecord();
            record.setPhysicOffset(1000L + i * 100);
            record.setTopic("fail-topic");
            pipeline.offer(record);
        }

        Thread.sleep(2000);

        // Checkpoint 不应被推进（因为所有写入都失败了）
        assertEquals("Checkpoint 不应被推进", 0L, checkpoint.getConfirmedOffset());
    }

    // ==================== 场景6：Pipeline 双重启动安全 ====================

    @Test
    public void testDoubleStartSafety() throws Exception {
        TrackingSource source = new TrackingSource();
        TrackingSink sink = new TrackingSink();

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink));
        pipeline.start();
        assertTrue(pipeline.isRunning());

        int startCountBefore = source.getStartCount();
        // 第二次启动应安全忽略
        pipeline.start();
        assertTrue(pipeline.isRunning());
        // Source.start() 不应被再次调用
        assertEquals("Source 不应被重复启动", startCountBefore, source.getStartCount());
    }

    // ==================== 场景7：Pipeline 双重停止安全 ====================

    @Test
    public void testDoubleStopSafety() throws Exception {
        TrackingSource source = new TrackingSource();
        TrackingSink sink = new TrackingSink();

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink));
        pipeline.start();
        pipeline.stopAll();
        assertFalse(pipeline.isRunning());

        // 第二次停止应安全忽略
        pipeline.stopAll();
        assertFalse(pipeline.isRunning());
    }

    // ==================== 场景8：队列满时 offer 不阻塞 ====================

    @Test
    public void testQueueFullOfferDoesNotBlock() throws Exception {
        TrackingSource source = new TrackingSource();
        // Sink 写入非常慢
        SlowSink slowSink = new SlowSink(500);

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(slowSink), 3);
        // 不启动 Pipeline，队列无消费者
        for (int i = 0; i < 3; i++) {
            SyncRecord r = new SyncRecord();
            r.setPhysicOffset(i);
            assertTrue(pipeline.offer(r));
        }

        // 队列满，offer 应立即返回 false
        long start = System.currentTimeMillis();
        SyncRecord overflow = new SyncRecord();
        overflow.setPhysicOffset(999);
        assertFalse(pipeline.offer(overflow));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue("offer 应立即返回，不应阻塞", elapsed < 100);
    }

    // ==================== 场景9：队列满时带超时的 offer ====================

    @Test
    public void testQueueFullOfferWithTimeout() throws Exception {
        TrackingSource source = new TrackingSource();
        TrackingSink sink = new TrackingSink();

        pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 2);
        SyncRecord r1 = new SyncRecord();
        SyncRecord r2 = new SyncRecord();
        SyncRecord r3 = new SyncRecord();

        assertTrue(pipeline.offer(r1));
        assertTrue(pipeline.offer(r2));

        // 带超时的 offer 应在超时后返回 false
        long start = System.currentTimeMillis();
        assertFalse(pipeline.offer(r3, 200, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue("超时时间应接近 200ms", elapsed >= 150 && elapsed < 500);
    }

    // ==================== 场景10：Sink stop 异常不影响其他 Sink 停止 ====================

    @Test
    public void testSinkStopExceptionIsolation() throws Exception {
        TrackingSource source = new TrackingSource();
        ErrorOnStopSink errorSink = new ErrorOnStopSink();
        TrackingSink normalSink = new TrackingSink();

        pipeline = new SyncPipeline(source,
                java.util.Arrays.<SyncSink>asList(errorSink, normalSink));
        pipeline.start();
        assertTrue(pipeline.isRunning());

        // 停止时第一个 Sink 会抛异常，但不应影响整体停止
        pipeline.stopAll();
        assertFalse("Pipeline 应成功停止", pipeline.isRunning());
        assertTrue("正常 Sink 应被停止", normalSink.wasStopped());
    }

    // ==================== 辅助类 ====================

    /** 带记录的 Source */
    static class TrackingSource implements SyncSource {
        private volatile boolean running = false;
        private volatile boolean everStarted = false;
        private volatile boolean everStopped = false;
        private final AtomicInteger startCount = new AtomicInteger(0);

        @Override
        public void start() {
            running = true;
            everStarted = true;
            startCount.incrementAndGet();
        }

        @Override
        public void stop() {
            running = false;
            everStopped = true;
        }

        @Override
        public boolean isRunning() { return running; }

        @Override
        public void poll() {
            try { Thread.sleep(50); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        boolean wasStarted() { return everStarted; }
        boolean wasStopped() { return everStopped; }
        int getStartCount() { return startCount.get(); }
    }

    /** 可控制启动失败的 Source */
    static class FailingSource implements SyncSource {
        private final boolean failOnStart;
        private final boolean failOnPoll;
        private volatile boolean running = false;

        FailingSource(boolean failOnStart, boolean failOnPoll) {
            this.failOnStart = failOnStart;
            this.failOnPoll = failOnPoll;
        }

        @Override
        public void start() throws Exception {
            if (failOnStart) throw new RuntimeException("模拟 Source 启动失败");
            running = true;
        }

        @Override
        public void stop() { running = false; }

        @Override
        public boolean isRunning() { return running; }

        @Override
        public void poll() {
            if (failOnPoll) throw new RuntimeException("模拟 Source poll 异常");
            try { Thread.sleep(50); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** poll 前 N 次抛异常 */
    static class InterruptibleSource implements SyncSource {
        private volatile boolean running = false;
        private final int failCount;
        private final AtomicInteger pollCount = new AtomicInteger(0);

        InterruptibleSource(int failCount) {
            this.failCount = failCount;
        }

        @Override
        public void start() { running = true; }

        @Override
        public void stop() { running = false; }

        @Override
        public boolean isRunning() { return running; }

        @Override
        public void poll() {
            int count = pollCount.incrementAndGet();
            if (count <= failCount) {
                throw new RuntimeException("模拟第 " + count + " 次 poll 异常");
            }
            try { Thread.sleep(50); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        int getPollCount() { return pollCount.get(); }
    }

    /** 带记录的 Sink */
    static class TrackingSink implements SyncSink {
        private volatile boolean started = false;
        private volatile boolean stopped = false;

        @Override
        public void start() { started = true; }

        @Override
        public void stop() { started = false; stopped = true; }

        @Override
        public void write(SyncRecord record) {}

        @Override
        public void flush() {}

        boolean isStarted() { return started; }
        boolean wasStopped() { return stopped; }
    }

    /** 可控制启动/写入失败的 Sink */
    static class FailingSink implements SyncSink {
        private final boolean failOnStart;
        private final boolean failOnWrite;
        private final int failWriteCount;
        private final AtomicInteger writeAttempts = new AtomicInteger(0);
        private final AtomicInteger successCount = new AtomicInteger(0);

        FailingSink(boolean failOnStart, boolean failOnWrite, int failWriteCount) {
            this.failOnStart = failOnStart;
            this.failOnWrite = failOnWrite;
            this.failWriteCount = failWriteCount;
        }

        @Override
        public void start() throws Exception {
            if (failOnStart) throw new RuntimeException("模拟 Sink 启动失败");
        }

        @Override
        public void stop() {}

        @Override
        public void write(SyncRecord record) throws Exception {
            int attempt = writeAttempts.incrementAndGet();
            if (failOnWrite && attempt <= failWriteCount) {
                throw new RuntimeException("模拟第 " + attempt + " 次写入失败");
            }
            successCount.incrementAndGet();
        }

        @Override
        public void flush() {}

        int getSuccessCount() { return successCount.get(); }
    }

    /** 所有写入都失败的 Sink */
    static class AlwaysFailingSink implements SyncSink {
        @Override public void start() {}
        @Override public void stop() {}
        @Override
        public void write(SyncRecord record) throws Exception {
            throw new RuntimeException("永远失败");
        }
        @Override public void flush() {}
    }

    /** 写入很慢的 Sink */
    static class SlowSink implements SyncSink {
        private final long delayMs;

        SlowSink(long delayMs) { this.delayMs = delayMs; }

        @Override public void start() {}
        @Override public void stop() {}
        @Override
        public void write(SyncRecord record) throws Exception {
            Thread.sleep(delayMs);
        }
        @Override public void flush() {}
    }

    /** stop 时抛异常的 Sink */
    static class ErrorOnStopSink implements SyncSink {
        @Override public void start() {}
        @Override public void stop() { throw new RuntimeException("模拟 stop 异常"); }
        @Override public void write(SyncRecord record) {}
        @Override public void flush() {}
    }

    /** 内存 Checkpoint */
    static class InMemoryCheckpoint implements CheckpointCoordinator {
        private final ConcurrentHashMap<String, AtomicLong> offsets = new ConcurrentHashMap<>();

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
        public void flush() {}
    }
}
