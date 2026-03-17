package org.apache.rocketmq.hasync.reliability;

import org.apache.rocketmq.hasync.core.SyncSink;
import org.apache.rocketmq.hasync.core.SyncSource;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * GracefulShutdownHandler 单元测试
 */
public class GracefulShutdownHandlerTest {

    private MetricsCollector metricsCollector;
    private boolean sourceStopped;
    private boolean sinkStopped;
    private boolean preCallbackCalled;
    private boolean postCallbackCalled;

    @Before
    public void setUp() {
        metricsCollector = new MetricsCollector();
        sourceStopped = false;
        sinkStopped = false;
        preCallbackCalled = false;
        postCallbackCalled = false;
    }

    @Test
    public void testShutdown() {
        SyncSource mockSource = createMockSource();
        SyncSink mockSink = createMockSink();

        GracefulShutdownHandler handler = new GracefulShutdownHandler(
                mockSource, Collections.singletonList(mockSink), null, metricsCollector);

        handler.shutdown();

        assertTrue(sourceStopped);
        assertTrue(sinkStopped);
        assertTrue(handler.isShutdownInitiated());
    }

    @Test
    public void testShutdownWithCallbacks() {
        SyncSource mockSource = createMockSource();
        SyncSink mockSink = createMockSink();

        GracefulShutdownHandler handler = new GracefulShutdownHandler(
                mockSource, Collections.singletonList(mockSink), null, metricsCollector);

        handler.setPreShutdownCallback(() -> preCallbackCalled = true);
        handler.setPostShutdownCallback(() -> postCallbackCalled = true);

        handler.shutdown();

        assertTrue(preCallbackCalled);
        assertTrue(postCallbackCalled);
    }

    @Test
    public void testDoubleShutdown() {
        SyncSource mockSource = createMockSource();
        GracefulShutdownHandler handler = new GracefulShutdownHandler(
                mockSource, new ArrayList<>(), null, metricsCollector);

        handler.shutdown();
        handler.shutdown(); // 第二次调用应该被忽略

        assertTrue(handler.isShutdownInitiated());
    }

    @Test
    public void testShutdownWithSnapshot() {
        SyncSource mockSource = createMockSource();
        SnapshotWriter writer = new SnapshotWriter("target/test-shutdown-snapshot", metricsCollector);

        GracefulShutdownHandler handler = new GracefulShutdownHandler(
                mockSource, new ArrayList<>(), writer, metricsCollector);

        handler.shutdown();

        assertTrue(handler.isShutdownInitiated());
    }

    @Test
    public void testMultipleSinks() {
        SyncSource mockSource = createMockSource();
        List<SyncSink> sinks = new ArrayList<>();
        final int[] stoppedCount = {0};

        for (int i = 0; i < 3; i++) {
            sinks.add(new SyncSink() {
                @Override public void start() {}
                @Override public void stop() { stoppedCount[0]++; }
                @Override public void write(SyncRecord record) {}
                @Override public void flush() {}
            });
        }

        GracefulShutdownHandler handler = new GracefulShutdownHandler(
                mockSource, sinks, null, metricsCollector);

        handler.shutdown();
        assertEquals(3, stoppedCount[0]);
    }

    // ==================== 辅助方法 ====================

    private SyncSource createMockSource() {
        return new SyncSource() {
            private boolean running = true;
            @Override public void start() { running = true; }
            @Override public void stop() { running = false; sourceStopped = true; }
            @Override public boolean isRunning() { return running; }
            @Override public void poll() {}
        };
    }

    private SyncSink createMockSink() {
        return new SyncSink() {
            @Override public void start() {}
            @Override public void stop() { sinkStopped = true; }
            @Override public void write(SyncRecord record) {}
            @Override public void flush() {}
        };
    }
}
