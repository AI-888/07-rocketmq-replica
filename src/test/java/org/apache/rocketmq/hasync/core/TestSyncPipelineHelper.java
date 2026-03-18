package org.apache.rocketmq.hasync.core;

import org.apache.rocketmq.hasync.model.SyncRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 测试专用的 Pipeline 辅助类 — 模拟 Source → Queue → Sink 数据流
 * <p>
 * <b>注意：此类仅用于单元测试和集成测试</b>，不用于生产环境。
 * 生产环境中 Source 与 Sink 之间统一通过 ZMQ REQ-REP 模式通信，
 * 参见 {@link SyncPipeline}。
 * <p>
 * 本类保留了旧的 BlockingQueue 内部队列机制，用于在测试中模拟
 * Source 产出数据、Sink 消费数据的完整流程，方便验证数据流的
 * 正确性、顺序性、Checkpoint 推进等逻辑。
 */
public class TestSyncPipelineHelper {

    private static final Logger log = LoggerFactory.getLogger(TestSyncPipelineHelper.class);

    /** 默认队列容量 */
    public static final int DEFAULT_QUEUE_CAPACITY = 1000;

    /** 队列 poll 超时（毫秒） */
    private static final long POLL_TIMEOUT_MS = 100;

    /** 数据源 */
    private final SyncSource source;

    /** 数据写入目标列表 */
    private final List<SyncSink> sinks;

    /** 活跃的 Sink 列表（可动态修改） */
    private final CopyOnWriteArrayList<SyncSink> activeSinks;

    /** Source → Sink 中转队列（测试用） */
    private final BlockingQueue<SyncRecord> queue;

    /** 运行标志 */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Source 拉取线程 */
    private Thread sourceThread;

    /** Sink 写入线程列表 */
    private final List<Thread> sinkThreads = new ArrayList<>();

    /** Sink 到线程的映射（用于动态移除） */
    private final java.util.concurrent.ConcurrentHashMap<SyncSink, Thread> sinkThreadMap =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * 构造测试 Pipeline（使用默认队列容量）
     */
    public TestSyncPipelineHelper(SyncSource source, List<SyncSink> sinks) {
        this(source, sinks, DEFAULT_QUEUE_CAPACITY);
    }

    /**
     * 构造测试 Pipeline
     */
    public TestSyncPipelineHelper(SyncSource source, List<SyncSink> sinks, int queueCapacity) {
        if (source == null) {
            throw new IllegalArgumentException("source 不能为 null");
        }
        if (sinks == null || sinks.isEmpty()) {
            throw new IllegalArgumentException("sinks 不能为空");
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity 必须大于 0");
        }
        this.source = source;
        this.sinks = Collections.unmodifiableList(new ArrayList<>(sinks));
        this.activeSinks = new CopyOnWriteArrayList<>(sinks);
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
    }

    /**
     * 启动 Pipeline
     */
    public void start() throws Exception {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        // 1. 启动 Source
        try {
            source.start();
        } catch (Exception e) {
            running.set(false);
            throw new RuntimeException("Source 启动失败", e);
        }

        // 2. 启动所有 Sink
        for (int i = 0; i < sinks.size(); i++) {
            try {
                sinks.get(i).start();
            } catch (Exception e) {
                stopAll();
                throw new RuntimeException("Sink[" + i + "] 启动失败", e);
            }
        }

        // 3. 启动 Source 拉取线程
        sourceThread = new Thread(() -> {
            while (running.get()) {
                try {
                    source.poll();
                } catch (Exception e) {
                    if (!running.get()) break;
                    try { Thread.sleep(1000); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt(); break;
                    }
                }
            }
        }, "source-poll-thread");
        sourceThread.setDaemon(true);
        sourceThread.start();

        // 4. 启动 Sink 消费线程
        for (int i = 0; i < sinks.size(); i++) {
            final int sinkIndex = i;
            final SyncSink sink = sinks.get(i);
            Thread sinkThread = new Thread(() -> {
                while (running.get() && activeSinks.contains(sink)) {
                    try {
                        SyncRecord record = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        if (record != null) {
                            sink.write(record);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        if (!running.get()) break;
                    }
                }
                // 优雅退出：处理队列中剩余消息
                if (!running.get()) {
                    drainRemaining(sink, sinkIndex);
                }
            }, "sink-write-thread-" + i);
            sinkThread.setDaemon(true);
            sinkThread.start();
            sinkThreads.add(sinkThread);
            sinkThreadMap.put(sink, sinkThread);
        }
    }

    /**
     * 停止 Pipeline
     */
    public void stopAll() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        if (sourceThread != null) {
            try { sourceThread.join(5000); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        for (Thread sinkThread : sinkThreads) {
            try { sinkThread.join(5000); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        sinkThreads.clear();

        for (int i = 0; i < sinks.size(); i++) {
            try { sinks.get(i).stop(); } catch (Exception e) { /* ignore */ }
        }

        try { source.stop(); } catch (Exception e) { /* ignore */ }
    }

    /**
     * 动态移除一个 Sink（缩容场景测试用）
     */
    public boolean removeSink(SyncSink sink) {
        if (!activeSinks.remove(sink)) {
            return false;
        }
        try { sink.stop(); } catch (Exception e) { /* ignore */ }
        Thread sinkThread = sinkThreadMap.remove(sink);
        if (sinkThread != null) {
            try { sinkThread.join(POLL_TIMEOUT_MS * 3); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return true;
    }

    public int getActiveSinkCount() {
        return activeSinks.size();
    }

    public List<SyncSink> getActiveSinks() {
        return Collections.unmodifiableList(new ArrayList<>(activeSinks));
    }

    /**
     * 向内部队列投递 SyncRecord（测试用）
     */
    public boolean offer(SyncRecord record) {
        return queue.offer(record);
    }

    /**
     * 向内部队列投递 SyncRecord（带超时，测试用）
     */
    public boolean offer(SyncRecord record, long timeout, TimeUnit unit) throws InterruptedException {
        return queue.offer(record, timeout, unit);
    }

    public boolean isRunning() {
        return running.get();
    }

    public int getQueueSize() {
        return queue.size();
    }

    public int getQueueRemainingCapacity() {
        return queue.remainingCapacity();
    }

    public SyncSource getSource() {
        return source;
    }

    public List<SyncSink> getSinks() {
        return sinks;
    }

    private void drainRemaining(SyncSink sink, int sinkIndex) {
        SyncRecord record;
        while ((record = queue.poll()) != null) {
            try { sink.write(record); } catch (Exception e) { break; }
        }
    }
}
