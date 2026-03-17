package org.apache.rocketmq.hasync.bootstrap;

import org.apache.rocketmq.hasync.config.SinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink 进程启动器
 * <p>
 * 启动流程：
 * <ol>
 *   <li>加载配置（三层合并）</li>
 *   <li>从 NameServer KV 发现 Source ZMQ 地址</li>
 *   <li>创建 RocketMQSink 实例（阶段四实现）</li>
 *   <li>创建 SyncPipeline 并启动</li>
 *   <li>注册 JVM ShutdownHook 实现优雅停机</li>
 * </ol>
 */
public class SinkBootstrap {

    private static final Logger log = LoggerFactory.getLogger(SinkBootstrap.class);

    public static void main(String[] args) {
        log.info("=== RocketMQ HA Sync — Sink 启动 ===");

        // 1. 加载配置
        SinkConfig config = new SinkConfig();
        config.load(args);

        log.info("Sink 配置加载完成");
        log.info("  目标集群 NameServer: {}", config.getTargetNamesrv());
        log.info("  Sink ID: {}", config.getSinkId());
        log.info("  批量大小: {}", config.getSinkBatchSize());
        log.info("  写入线程数: {}", config.getSinkThreads());

        // 2. 创建 Sink 和 Pipeline（阶段四实现具体类）
        // TODO: 阶段四 — 从 NameServer KV 发现 Source ZMQ 地址
        // TODO: 阶段四 — 创建 RocketMQSink 实例
        // TODO: 阶段四 — 创建 SyncPipeline 并启动

        // 3. 注册 ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("收到停机信号，开始优雅退出...");
            // TODO: 阶段四 — pipeline.stopAll()
            log.info("Sink 优雅退出完成");
        }, "sink-shutdown-hook"));

        log.info("=== Sink 已启动，等待数据写入 ===");
    }
}
