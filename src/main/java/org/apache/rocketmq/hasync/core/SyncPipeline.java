package org.apache.rocketmq.hasync.core;

import org.apache.rocketmq.hasync.model.SyncRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 管道编排 — 组装 Source 和 Sink，管理完整的数据同步生命周期
 * <p>
 * 对应需求 2 §2（SyncPipeline 管道）：
 * <ol>
 *   <li>组装 Source 和 Sink 组件</li>
 *   <li>管理启动、停止顺序</li>
 *   <li>Source 线程从 Master 拉取数据放入内部队列</li>
 *   <li>Sink 线程从队列消费写入目标</li>
 * </ol>
 * <p>
 * 内部使用 {@link BlockingQueue} 作为缓冲区（默认容量 1000）
 *
 * @see SyncSource
 * @see SyncSink
 */
public class SyncPipeline {

    private static final Logger log = LoggerFactory.getLogger(SyncPipeline.class);

    /** 默认队列容量 */
    public static final int DEFAULT_QUEUE_CAPACITY = 1000;

    /** 队列 poll 超时（毫秒） */
    private static final long POLL_TIMEOUT_MS = 100;

    /** 数据源 */
    private final SyncSource source;

    /** 数据写入目标列表 */
    private final List<SyncSink> sinks;

    /** Source → Sink 中转队列 */
    private final BlockingQueue<SyncRecord> queue;

    /** 运行标志 */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Source 拉取线程 */
    private Thread sourceThread;

    /** Sink 写入线程列表 */
    private final List<Thread> sinkThreads = new ArrayList<>();

    /**
     * 构造管道（使用默认队列容量）
     *
     * @param source 数据源
     * @param sinks  数据写入目标列表
     */
    public SyncPipeline(SyncSource source, List<SyncSink> sinks) {
        this(source, sinks, DEFAULT_QUEUE_CAPACITY);
    }

    /**
     * 构造管道
     *
     * @param source        数据源
     * @param sinks         数据写入目标列表
     * @param queueCapacity 内部队列容量
     */
    public SyncPipeline(SyncSource source, List<SyncSink> sinks, int queueCapacity) {
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
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
    }

    /**
     * 启动管道
     * <p>
     * 启动顺序：
     * <ol>
     *   <li>启动 Source</li>
     *   <li>启动所有 Sink</li>
     *   <li>启动 Source 拉取线程</li>
     *   <li>启动 Sink 消费线程</li>
     * </ol>
     *
     * @throws Exception 启动失败
     */
    public void start() throws Exception {
        if (!running.compareAndSet(false, true)) {
            log.warn("SyncPipeline 已在运行中，忽略重复启动");
            return;
        }

        log.info("SyncPipeline 启动中...");

        // 1. 启动 Source
        try {
            source.start();
            log.info("Source 启动成功");
        } catch (Exception e) {
            running.set(false);
            throw new RuntimeException("Source 启动失败", e);
        }

        // 2. 启动所有 Sink
        for (int i = 0; i < sinks.size(); i++) {
            try {
                sinks.get(i).start();
                log.info("Sink[{}] 启动成功", i);
            } catch (Exception e) {
                // 回滚：停止已启动的 Sink 和 Source
                log.error("Sink[{}] 启动失败，回滚已启动组件", i, e);
                stopAll();
                throw new RuntimeException("Sink[" + i + "] 启动失败", e);
            }
        }

        // 3. 启动 Source 拉取线程
        sourceThread = new Thread(() -> {
            log.info("Source 拉取线程已启动");
            while (running.get()) {
                try {
                    source.poll();
                } catch (Exception e) {
                    if (!running.get()) {
                        break;
                    }
                    log.error("Source poll 异常", e);
                    // 短暂休眠避免紧密循环
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            log.info("Source 拉取线程已退出");
        }, "source-poll-thread");
        sourceThread.setDaemon(true);
        sourceThread.start();

        // 4. 启动 Sink 消费线程
        for (int i = 0; i < sinks.size(); i++) {
            final int sinkIndex = i;
            final SyncSink sink = sinks.get(i);
            Thread sinkThread = new Thread(() -> {
                log.info("Sink[{}] 消费线程已启动", sinkIndex);
                while (running.get()) {
                    try {
                        SyncRecord record = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        if (record != null) {
                            sink.write(record);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        if (!running.get()) {
                            break;
                        }
                        log.error("Sink[{}] write 异常", sinkIndex, e);
                    }
                }
                // 优雅退出：处理队列中剩余消息
                drainRemaining(sink, sinkIndex);
                log.info("Sink[{}] 消费线程已退出", sinkIndex);
            }, "sink-write-thread-" + i);
            sinkThread.setDaemon(true);
            sinkThread.start();
            sinkThreads.add(sinkThread);
        }

        log.info("SyncPipeline 启动完成（Source: 1, Sinks: {}, QueueCapacity: {}）",
                sinks.size(), queue.remainingCapacity() + queue.size());
    }

    /**
     * 停止管道（优雅停机）
     * <p>
     * 停止顺序（需求 17）：
     * <ol>
     *   <li>设置 running=false</li>
     *   <li>等待 Source 线程退出</li>
     *   <li>等待 Sink 线程处理完剩余数据后退出</li>
     *   <li>停止 Sink</li>
     *   <li>停止 Source</li>
     * </ol>
     */
    public void stopAll() {
        if (!running.compareAndSet(true, false)) {
            log.warn("SyncPipeline 未在运行，忽略停止请求");
            return;
        }

        log.info("SyncPipeline 停止中...");

        // 1. 等待 Source 线程退出
        if (sourceThread != null) {
            try {
                sourceThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 2. 等待 Sink 线程退出
        for (Thread sinkThread : sinkThreads) {
            try {
                sinkThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        sinkThreads.clear();

        // 3. 停止所有 Sink
        for (int i = 0; i < sinks.size(); i++) {
            try {
                sinks.get(i).stop();
                log.info("Sink[{}] 已停止", i);
            } catch (Exception e) {
                log.error("Sink[{}] 停止异常", i, e);
            }
        }

        // 4. 停止 Source
        try {
            source.stop();
            log.info("Source 已停止");
        } catch (Exception e) {
            log.error("Source 停止异常", e);
        }

        log.info("SyncPipeline 已完全停止");
    }

    /**
     * 向内部队列投递 SyncRecord（供 Source 内部调用）
     *
     * @param record 待投递的记录
     * @return true 投递成功，false 队列已满
     */
    public boolean offer(SyncRecord record) {
        return queue.offer(record);
    }

    /**
     * 向内部队列投递 SyncRecord（带超时）
     *
     * @param record  待投递的记录
     * @param timeout 超时时间
     * @param unit    时间单位
     * @return true 投递成功，false 超时
     * @throws InterruptedException 中断异常
     */
    public boolean offer(SyncRecord record, long timeout, TimeUnit unit) throws InterruptedException {
        return queue.offer(record, timeout, unit);
    }

    /**
     * 检查管道是否正在运行
     *
     * @return true 表示运行中
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 获取当前队列中待处理记录数
     *
     * @return 队列大小
     */
    public int getQueueSize() {
        return queue.size();
    }

    /**
     * 获取队列剩余容量
     *
     * @return 剩余容量
     */
    public int getQueueRemainingCapacity() {
        return queue.remainingCapacity();
    }

    /**
     * 获取数据源
     */
    public SyncSource getSource() {
        return source;
    }

    /**
     * 获取 Sink 列表（不可修改）
     */
    public List<SyncSink> getSinks() {
        return sinks;
    }

    // ==================== 内部方法 ====================

    /**
     * 处理队列中剩余的消息（优雅停机）
     */
    private void drainRemaining(SyncSink sink, int sinkIndex) {
        int drained = 0;
        SyncRecord record;
        while ((record = queue.poll()) != null) {
            try {
                sink.write(record);
                drained++;
            } catch (Exception e) {
                log.error("Sink[{}] drain 剩余消息异常", sinkIndex, e);
                break;
            }
        }
        if (drained > 0) {
            log.info("Sink[{}] drain 处理了 {} 条剩余消息", sinkIndex, drained);
        }
    }
}
