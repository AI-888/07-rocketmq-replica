package org.apache.rocketmq.hasync.core;

import org.apache.rocketmq.hasync.config.SinkConfig;
import org.apache.rocketmq.hasync.config.SourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 内嵌 Sink 启动器 — 在 Source 同进程内启动 Sink 实例（--with-sink 模式）
 * <p>
 * <b>设计原则</b>：删除原有的 BlockingQueue 单进程模式。Source 与 Sink 之间
 * <b>统一</b>通过 ZeroMQ REQ-REP 模式通信。同进程模式下，Sink 通过
 * {@code localhost:{zmqPort}} 连接 Source ZMQ REP Socket，通信协议和
 * 服务发现逻辑与独立部署完全相同，消除维护差异。
 * <p>
 * 对应需求 2 §2：
 * <ol>
 *   <li>Source 启动时可通过 --with-sink 参数在同进程内嵌启动 Sink</li>
 *   <li>内嵌 Sink 通过 localhost ZMQ 连接 Source，通信逻辑与独立部署一致</li>
 *   <li>当 Source 或 Sink 发生不可恢复异常时，记录 ERROR 日志并停止相关组件</li>
 * </ol>
 *
 * @see SyncSource
 * @see SyncSink
 */
public class SyncPipeline {

    private static final Logger log = LoggerFactory.getLogger(SyncPipeline.class);

    /** 数据源 */
    private final SyncSource source;

    /** 内嵌 Sink 列表 */
    private final List<SyncSink> sinks;

    /** 运行标志 */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Source 拉取线程 */
    private Thread sourceThread;

    /** Sink 拉取线程列表（每个 Sink 有自己的 ZMQ REQ 连接） */
    private final List<Thread> sinkThreads = new ArrayList<>();

    /**
     * 构造内嵌 Sink 启动器
     *
     * @param source 数据源（HASource，已绑定 ZMQ REP Socket）
     * @param sinks  内嵌 Sink 列表（通过 localhost ZMQ 连接 Source）
     */
    public SyncPipeline(SyncSource source, List<SyncSink> sinks) {
        if (source == null) {
            throw new IllegalArgumentException("source 不能为 null");
        }
        if (sinks == null || sinks.isEmpty()) {
            throw new IllegalArgumentException("sinks 不能为空");
        }
        this.source = source;
        this.sinks = Collections.unmodifiableList(new ArrayList<>(sinks));
    }

    /**
     * 启动 Source 和内嵌 Sink
     * <p>
     * 启动顺序：
     * <ol>
     *   <li>启动 Source（含 ZMQ REP Socket 绑定）</li>
     *   <li>启动所有内嵌 Sink（各自通过 ZMQ REQ 连接 localhost:{zmqPort}）</li>
     *   <li>启动 Source 拉取线程</li>
     *   <li>启动 Sink 拉取线程（各自独立通过 ZMQ 从 Source 拉取数据）</li>
     * </ol>
     *
     * @throws Exception 启动失败
     */
    public void start() throws Exception {
        if (!running.compareAndSet(false, true)) {
            log.warn("SyncPipeline 已在运行中，忽略重复启动");
            return;
        }

        log.info("SyncPipeline 启动中（统一 ZMQ 通信模式）...");

        // 1. 启动 Source（绑定 ZMQ REP Socket）
        try {
            source.start();
            log.info("Source 启动成功（ZMQ REP Socket 已绑定）");
        } catch (Exception e) {
            running.set(false);
            throw new RuntimeException("Source 启动失败", e);
        }

        // 2. 启动所有内嵌 Sink（各自创建 ZMQ REQ 连接）
        for (int i = 0; i < sinks.size(); i++) {
            try {
                sinks.get(i).start();
                log.info("内嵌 Sink[{}] 启动成功（ZMQ REQ 已连接 Source）", i);
            } catch (Exception e) {
                log.error("内嵌 Sink[{}] 启动失败，回滚已启动组件", i, e);
                stopAll();
                throw new RuntimeException("Sink[" + i + "] 启动失败", e);
            }
        }

        // 3. 启动 Source 拉取线程（从 Master 拉取数据到内存缓冲区）
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

        // 4. 启动 Sink 拉取线程（各自通过 ZMQ REQ 从 Source 拉取数据）
        for (int i = 0; i < sinks.size(); i++) {
            final int sinkIndex = i;
            final SyncSink sink = sinks.get(i);
            Thread sinkThread = new Thread(() -> {
                log.info("内嵌 Sink[{}] 拉取线程已启动（通过 ZMQ 连接 Source）", sinkIndex);
                while (running.get()) {
                    try {
                        // Sink 内部通过 ZMQ REQ 发送 PullRequest 并接收 PullResponse
                        // 然后调用 write() 写入目标集群
                        // 这与独立部署的 Sink 逻辑完全相同
                        sink.write(null); // Sink 内部处理 ZMQ 拉取逻辑
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        if (!running.get()) {
                            break;
                        }
                        log.error("内嵌 Sink[{}] 处理异常", sinkIndex, e);
                    }
                }
                log.info("内嵌 Sink[{}] 拉取线程已退出", sinkIndex);
            }, "embedded-sink-thread-" + i);
            sinkThread.setDaemon(true);
            sinkThread.start();
            sinkThreads.add(sinkThread);
        }

        log.info("SyncPipeline 启动完成（Source: 1, 内嵌 Sinks: {}，通信模式: ZMQ REQ-REP）",
                sinks.size());
    }

    /**
     * 停止 Source 和所有内嵌 Sink（优雅停机）
     * <p>
     * 停止顺序（需求 17）：
     * <ol>
     *   <li>设置 running=false</li>
     *   <li>等待 Source 线程退出</li>
     *   <li>等待 Sink 线程退出</li>
     *   <li>停止所有 Sink</li>
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

        // 3. 停止所有内嵌 Sink
        for (int i = 0; i < sinks.size(); i++) {
            try {
                sinks.get(i).stop();
                log.info("内嵌 Sink[{}] 已停止", i);
            } catch (Exception e) {
                log.error("内嵌 Sink[{}] 停止异常", i, e);
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
     * 为内嵌 Sink 构建配置
     * <p>
     * 从 SourceConfig 派生 SinkConfig：
     * <ul>
     *   <li>targetNamesrv 继承自 SourceConfig</li>
     *   <li>Sink 通过 localhost:{zmqPort} 连接同进程的 Source ZMQ Socket</li>
     *   <li>服务发现和通信逻辑与独立部署完全一致</li>
     * </ul>
     *
     * @param sourceConfig Source 配置
     * @return Sink 配置（预配置为连接 localhost）
     */
    public static SinkConfig buildEmbeddedSinkConfig(SourceConfig sourceConfig) {
        SinkConfig sinkConfig = new SinkConfig();
        // Sink 配置继承 Source 的目标集群地址
        // 实际连接时通过 NameServer KV 发现 Source ZMQ 地址
        // 由于 Source 已将 localhost:{zmqPort} 注册到 KV，Sink 自然连接到同进程 Source
        String[] args = new String[]{
                "--targetNamesrv", sourceConfig.getTargetNamesrv(),
                "--sinkId", "embedded-sink-" + sourceConfig.getSourceNodeId()
        };
        sinkConfig.load(args);
        return sinkConfig;
    }

    /**
     * 检查是否正在运行
     *
     * @return true 表示运行中
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 获取数据源
     */
    public SyncSource getSource() {
        return source;
    }

    /**
     * 获取内嵌 Sink 列表（不可修改）
     */
    public List<SyncSink> getSinks() {
        return sinks;
    }
}
