package org.apache.rocketmq.hasync.bootstrap;

import org.apache.rocketmq.hasync.config.SinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink 进程启动器
 * <p>
 * <b>统一 ZMQ 通信模型：</b> 无论 Sink 是独立部署还是由 Source --with-sink 内嵌启动，
 * 均通过 ZMQ REQ Socket 从 Source 拉取数据，通信协议和服务发现逻辑完全一致。
 * <p>
 * 启动流程：
 * <ol>
 *   <li>加载配置（三层合并）</li>
 *   <li>从 NameServer KV 发现 Source ZMQ 地址</li>
 *   <li>创建 RocketMQSink 实例（通过 ZMQ REQ 连接 Source）</li>
 *   <li>启动 Sink 拉取和写入循环</li>
 *   <li>注册 JVM ShutdownHook 实现优雅停机</li>
 * </ol>
 */
public class SinkBootstrap {

    private static final Logger log = LoggerFactory.getLogger(SinkBootstrap.class);

    public static void main(String[] args) {
        log.info("=== RocketMQ HA Sync — Sink 启动 ===");
        log.info("[通信] 通过 ZMQ REQ-REP 模式从 Source 拉取数据");

        // 1. 加载配置
        SinkConfig config = new SinkConfig();
        config.load(args);

        log.info("Sink 配置加载完成");
        log.info("  目标集群 NameServer: {}", config.getTargetNamesrv());
        log.info("  Sink ID: {}", config.getSinkId());
        log.info("  批量大小: {}", config.getSinkBatchSize());
        log.info("  写入线程数: {}", config.getSinkThreads());

        // 2. 创建 Sink（阶段四实现具体类）
        // TODO: 阶段四 — 从 NameServer KV 发现 Source ZMQ 地址
        // TODO: 阶段四 — 创建 RocketMQSink 实例（ZMQ REQ 连接 Source）
        // TODO: 阶段四 — 启动 Sink 拉取和写入循环

        // 3. 注册 ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("收到停机信号，开始优雅退出...");
            // TODO: 阶段四 — 停止 Sink
            log.info("Sink 优雅退出完成");
        }, "sink-shutdown-hook"));

        log.info("=== Sink 已启动，等待数据写入 ===");
    }

    /**
     * 内嵌模式启动 Sink（由 SourceBootstrap --with-sink 调用）
     * <p>
     * 通过 localhost:{zmqPort} 连接同进程的 Source ZMQ Socket，
     * 通信协议和服务发现逻辑与独立部署完全一致。
     *
     * @param config Sink 配置（由 SyncPipeline.buildEmbeddedSinkConfig 构建）
     */
    public static void startEmbeddedSink(SinkConfig config) {
        log.info("=== 内嵌 Sink 启动 ===");
        log.info("  Sink ID: {}", config.getSinkId());
        log.info("  通信模式: ZMQ REQ-REP（与独立部署一致）");

        // TODO: 阶段四 — 创建 RocketMQSink 并启动
        // Sink 内部通过 NameServer KV 发现 Source ZMQ 地址（localhost:{zmqPort}）
        // 然后通过 ZMQ REQ Socket 发送 PullRequest 拉取数据

        log.info("=== 内嵌 Sink 已启动 ===");
    }
}
