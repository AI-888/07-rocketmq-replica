package org.apache.rocketmq.hasync.core;

import org.apache.rocketmq.hasync.model.SyncRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * SyncPipeline 管道编排单元测试
 */
public class SyncPipelineTest {

    private MockSyncSource mockSource;
    private MockSyncSink mockSink;
    private SyncPipeline pipeline;

    @Before
    public void setUp() {
        mockSource = new MockSyncSource();
        mockSink = new MockSyncSink();
    }

    @After
    public void tearDown() {
        if (pipeline != null && pipeline.isRunning()) {
            pipeline.stopAll();
        }
    }

    // ==================== 构造函数测试 ====================

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullSource() {
        new SyncPipeline(null, Collections.singletonList(mockSink));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullSinks() {
        new SyncPipeline(mockSource, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptySinks() {
        new SyncPipeline(mockSource, Collections.<SyncSink>emptyList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroQueueCapacity() {
        new SyncPipeline(mockSource, Collections.singletonList(mockSink), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeQueueCapacity() {
        new SyncPipeline(mockSource, Collections.singletonList(mockSink), -1);
    }

    @Test
    public void testConstructorSuccess() {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink), 500);
        assertNotNull(pipeline);
        assertFalse(pipeline.isRunning());
        assertEquals(0, pipeline.getQueueSize());
    }

    @Test
    public void testConstructorDefaultCapacity() {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink));
        assertEquals(SyncPipeline.DEFAULT_QUEUE_CAPACITY, pipeline.getQueueRemainingCapacity());
    }

    // ==================== 启动和停止测试 ====================

    @Test
    public void testStartAndStop() throws Exception {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink));
        assertFalse(pipeline.isRunning());

        pipeline.start();
        assertTrue(pipeline.isRunning());
        assertTrue(mockSource.isStarted());
        assertTrue(mockSink.isStarted());

        pipeline.stopAll();
        assertFalse(pipeline.isRunning());
    }

    @Test
    public void testDoubleStartIgnored() throws Exception {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink));
        pipeline.start();
        assertTrue(pipeline.isRunning());

        // 第二次调用应安全忽略
        pipeline.start();
        assertTrue(pipeline.isRunning());
    }

    @Test
    public void testStopWhenNotRunning() {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink));
        // 未启动时停止应安全忽略
        pipeline.stopAll();
        assertFalse(pipeline.isRunning());
    }

    // ==================== Source 启动失败回滚 ====================

    @Test
    public void testSourceStartFailure() throws Exception {
        MockSyncSource failSource = new MockSyncSource();
        failSource.setStartShouldFail(true);

        pipeline = new SyncPipeline(failSource, Collections.singletonList(mockSink));
        try {
            pipeline.start();
            fail("应该抛出异常");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Source 启动失败"));
        }
        assertFalse(pipeline.isRunning());
    }

    // ==================== Sink 启动失败回滚 ====================

    @Test
    public void testSinkStartFailureRollback() throws Exception {
        MockSyncSink failSink = new MockSyncSink();
        failSink.setStartShouldFail(true);

        pipeline = new SyncPipeline(mockSource, Collections.singletonList(failSink));
        try {
            pipeline.start();
            fail("应该抛出异常");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Sink[0] 启动失败"));
        }
        assertFalse(pipeline.isRunning());
    }

    // ==================== 队列操作测试 ====================

    @Test
    public void testOfferAndQueueSize() {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink), 10);

        SyncRecord record = new SyncRecord();
        record.setTopic("test");
        assertTrue(pipeline.offer(record));
        assertEquals(1, pipeline.getQueueSize());
    }

    @Test
    public void testOfferWhenQueueFull() {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink), 2);

        SyncRecord r1 = new SyncRecord();
        SyncRecord r2 = new SyncRecord();
        SyncRecord r3 = new SyncRecord();

        assertTrue(pipeline.offer(r1));
        assertTrue(pipeline.offer(r2));
        // 队列已满
        assertFalse(pipeline.offer(r3));
        assertEquals(2, pipeline.getQueueSize());
    }

    @Test
    public void testOfferWithTimeout() throws InterruptedException {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink), 1);

        SyncRecord r1 = new SyncRecord();
        SyncRecord r2 = new SyncRecord();

        assertTrue(pipeline.offer(r1, 100, TimeUnit.MILLISECONDS));
        // 队列已满，超时返回 false
        assertFalse(pipeline.offer(r2, 100, TimeUnit.MILLISECONDS));
    }

    // ==================== Getter 测试 ====================

    @Test
    public void testGetSourceAndSinks() {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink));
        assertSame(mockSource, pipeline.getSource());
        assertEquals(1, pipeline.getSinks().size());
        assertSame(mockSink, pipeline.getSinks().get(0));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSinksListIsUnmodifiable() {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink));
        pipeline.getSinks().add(new MockSyncSink());
    }

    // ==================== 多 Sink 测试 ====================

    @Test
    public void testMultipleSinks() throws Exception {
        MockSyncSink sink1 = new MockSyncSink();
        MockSyncSink sink2 = new MockSyncSink();
        List<SyncSink> sinks = Arrays.<SyncSink>asList(sink1, sink2);

        pipeline = new SyncPipeline(mockSource, sinks);
        pipeline.start();

        assertTrue(sink1.isStarted());
        assertTrue(sink2.isStarted());
        assertEquals(2, pipeline.getSinks().size());

        pipeline.stopAll();
    }

    // ==================== 数据流测试 ====================

    @Test
    public void testDataFlow() throws Exception {
        final CountDownLatch writeLatch = new CountDownLatch(1);
        final AtomicBoolean writeReceived = new AtomicBoolean(false);

        MockSyncSink dataSink = new MockSyncSink() {
            @Override
            public void write(SyncRecord record) throws Exception {
                writeReceived.set(true);
                writeLatch.countDown();
            }
        };

        pipeline = new SyncPipeline(mockSource, Collections.<SyncSink>singletonList(dataSink), 100);
        pipeline.start();

        // 向队列投递数据
        SyncRecord record = new SyncRecord();
        record.setTopic("test-topic");
        record.setPhysicOffset(1000L);
        pipeline.offer(record);

        // 等待 Sink 处理
        assertTrue("Sink 应在超时前收到数据", writeLatch.await(5, TimeUnit.SECONDS));
        assertTrue(writeReceived.get());

        pipeline.stopAll();
    }

    // ==================== QueueRemainingCapacity 测试 ====================

    @Test
    public void testQueueRemainingCapacity() {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink), 50);
        assertEquals(50, pipeline.getQueueRemainingCapacity());

        pipeline.offer(new SyncRecord());
        assertEquals(49, pipeline.getQueueRemainingCapacity());
    }

    // ==================== Mock 实现 ====================

    /**
     * Mock SyncSource 实现
     */
    static class MockSyncSource implements SyncSource {
        private volatile boolean started = false;
        private boolean startShouldFail = false;

        @Override
        public void start() throws Exception {
            if (startShouldFail) {
                throw new RuntimeException("模拟 Source 启动失败");
            }
            started = true;
        }

        @Override
        public void stop() {
            started = false;
        }

        @Override
        public boolean isRunning() {
            return started;
        }

        @Override
        public void poll() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public boolean isStarted() {
            return started;
        }

        public void setStartShouldFail(boolean startShouldFail) {
            this.startShouldFail = startShouldFail;
        }
    }

    /**
     * Mock SyncSink 实现
     */
    static class MockSyncSink implements SyncSink {
        private volatile boolean started = false;
        private boolean startShouldFail = false;
        private final AtomicInteger writeCount = new AtomicInteger(0);

        @Override
        public void start() throws Exception {
            if (startShouldFail) {
                throw new RuntimeException("模拟 Sink 启动失败");
            }
            started = true;
        }

        @Override
        public void stop() {
            started = false;
        }

        @Override
        public void write(SyncRecord record) throws Exception {
            writeCount.incrementAndGet();
        }

        @Override
        public void flush() throws Exception {
        }

        public boolean isStarted() {
            return started;
        }

        public void setStartShouldFail(boolean startShouldFail) {
            this.startShouldFail = startShouldFail;
        }

        public int getWriteCount() {
            return writeCount.get();
        }
    }
}
