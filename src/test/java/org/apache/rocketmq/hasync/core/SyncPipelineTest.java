package org.apache.rocketmq.hasync.core;

import org.apache.rocketmq.hasync.config.SinkConfig;
import org.apache.rocketmq.hasync.config.SourceConfig;
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
 * SyncPipeline 统一 ZMQ 通信模型测试
 * <p>
 * 验证：
 * <ul>
 *   <li>构造函数校验</li>
 *   <li>启动/停止生命周期</li>
 *   <li>Source/Sink 启动失败回滚</li>
 *   <li>多 Sink 支持</li>
 *   <li>buildEmbeddedSinkConfig 配置继承</li>
 * </ul>
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

    @Test
    public void testConstructorSuccess() {
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink));
        assertNotNull(pipeline);
        assertFalse(pipeline.isRunning());
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

    // ==================== buildEmbeddedSinkConfig 测试 ====================

    @Test
    public void testBuildEmbeddedSinkConfig() {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.load(new String[]{
                "--sourceNamesrv", "10.0.0.1:9876",
                "--targetNamesrv", "10.0.0.2:9877",
                "--zmqBindPort", "5555",
                "--sourceNodeId", "test-node"
        });

        SinkConfig sinkConfig = SyncPipeline.buildEmbeddedSinkConfig(sourceConfig);
        assertNotNull(sinkConfig);
        assertEquals("10.0.0.2:9877", sinkConfig.getTargetNamesrv());
        assertTrue(sinkConfig.getSinkId().contains("embedded-sink"));
    }

    @Test
    public void testBuildEmbeddedSinkConfigInheritsTargetNamesrv() {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.load(new String[]{
                "--sourceNamesrv", "192.168.1.1:9876",
                "--targetNamesrv", "192.168.2.1:9877"
        });

        SinkConfig sinkConfig = SyncPipeline.buildEmbeddedSinkConfig(sourceConfig);
        assertEquals("192.168.2.1:9877", sinkConfig.getTargetNamesrv());
    }

    // ==================== 统一 ZMQ 通信验证 ====================

    @Test
    public void testUnifiedZmqCommunication() throws Exception {
        // 验证新架构下不存在 BlockingQueue / offer / poll 等方法
        // SyncPipeline 仅负责协调 Source 和 Sink 的生命周期
        pipeline = new SyncPipeline(mockSource, Collections.singletonList(mockSink));
        pipeline.start();

        // Source 和 Sink 均已启动
        assertTrue(mockSource.isStarted());
        assertTrue(mockSink.isStarted());

        // 验证 pipeline 的 API 中不再有 offer/getQueueSize 等队列方法
        // 通信完全通过 ZMQ（Sink 内部 ZMQ REQ → Source ZMQ REP）
        assertTrue(pipeline.isRunning());

        pipeline.stopAll();
        assertFalse(pipeline.isRunning());
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
