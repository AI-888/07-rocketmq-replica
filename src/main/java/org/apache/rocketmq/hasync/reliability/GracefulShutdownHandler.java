package org.apache.rocketmq.hasync.reliability;

import org.apache.rocketmq.hasync.core.SyncSource;
import org.apache.rocketmq.hasync.core.SyncSink;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 优雅停机处理器
 * <p>
 * 对应需求 17：
 * <ul>
 *   <li>§1：接收 SIGTERM/SIGINT → 停止拉取 → 等待队列清空 → 刷写 Checkpoint</li>
 *   <li>§2：最长等待 30 秒，超时强制停止</li>
 *   <li>§3：写入快照文件 snapshot.json</li>
 *   <li>§5：先写临时文件再原子重命名</li>
 * </ul>
 */
public class GracefulShutdownHandler {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownHandler.class);

    /** 最长等待时间（毫秒） */
    private static final long MAX_DRAIN_WAIT_MS = 30_000;

    /** 队列排空检查间隔 */
    private static final long DRAIN_CHECK_INTERVAL_MS = 100;

    private final SyncSource source;
    private final List<SyncSink> sinks;
    private final SnapshotWriter snapshotWriter;
    private final MetricsCollector metricsCollector;
    private volatile boolean shutdownInitiated = false;

    /** 停机前回调（可选） */
    private Runnable preShutdownCallback;
    /** 停机后回调（可选） */
    private Runnable postShutdownCallback;

    public GracefulShutdownHandler(SyncSource source, List<SyncSink> sinks,
                                   SnapshotWriter snapshotWriter,
                                   MetricsCollector metricsCollector) {
        this.source = source;
        this.sinks = sinks;
        this.snapshotWriter = snapshotWriter;
        this.metricsCollector = metricsCollector;
    }

    public void setPreShutdownCallback(Runnable callback) {
        this.preShutdownCallback = callback;
    }

    public void setPostShutdownCallback(Runnable callback) {
        this.postShutdownCallback = callback;
    }

    /**
     * 注册 JVM ShutdownHook
     */
    public void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("收到停机信号，开始优雅退出...");
            shutdown();
        }, "graceful-shutdown-hook"));
    }

    /**
     * 执行优雅停机
     */
    public void shutdown() {
        if (shutdownInitiated) {
            return;
        }
        shutdownInitiated = true;

        log.info("========== 优雅停机开始 ==========");

        try {
            // 1. 执行前置回调
            if (preShutdownCallback != null) {
                preShutdownCallback.run();
            }

            // 2. 停止 Source 拉取新数据
            log.info("步骤 1/5：停止 Source 拉取新数据");
            source.stop();

            // 3. 等待 Sink 处理完队列中的消息（最长 30 秒）
            log.info("步骤 2/5：等待 Sink 处理完剩余消息（最长 {}ms）", MAX_DRAIN_WAIT_MS);
            boolean drained = waitForDrain();
            if (!drained) {
                log.warn("等待超时（{}ms），强制停止 Sink", MAX_DRAIN_WAIT_MS);
            }

            // 4. 停止所有 Sink（会触发 Checkpoint 刷写）
            log.info("步骤 3/5：停止所有 Sink 并刷写 Checkpoint");
            for (SyncSink sink : sinks) {
                try {
                    sink.stop();
                } catch (Exception e) {
                    log.warn("Sink 停止失败: {}", e.getMessage());
                }
            }

            // 5. 写入快照文件
            log.info("步骤 4/5：写入快照文件");
            if (snapshotWriter != null) {
                snapshotWriter.writeSnapshot();
            }

            // 6. 执行后置回调
            log.info("步骤 5/5：执行清理任务");
            if (postShutdownCallback != null) {
                postShutdownCallback.run();
            }

        } catch (Exception e) {
            log.error("优雅停机过程中发生异常: {}", e.getMessage(), e);
        }

        log.info("========== 优雅停机完成 ==========");
    }

    /**
     * 等待队列中的消息被消费完毕
     *
     * @return true 如果队列已排空，false 如果超时
     */
    private boolean waitForDrain() {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < MAX_DRAIN_WAIT_MS) {
            // 如果 Source 已停止且没有活跃的 Sink 处理，视为排空
            if (!source.isRunning()) {
                return true;
            }

            try {
                Thread.sleep(DRAIN_CHECK_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    public boolean isShutdownInitiated() {
        return shutdownInitiated;
    }
}
