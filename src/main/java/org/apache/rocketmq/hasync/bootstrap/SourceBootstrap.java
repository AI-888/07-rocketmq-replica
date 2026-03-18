package org.apache.rocketmq.hasync.bootstrap;

import org.apache.rocketmq.hasync.config.SinkConfig;
import org.apache.rocketmq.hasync.config.SourceConfig;
import org.apache.rocketmq.hasync.core.SyncPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source 进程启动器
 * <p>
 * <b>重要约束（需求 3 — 不参与选举）：</b>
 * <ul>
 *   <li>以「虚拟 Slave」身份连接 Master，仅接收 CommitLog 数据流</li>
 *   <li>不注册为合法 Slave，不参与 Controller 选举/投票</li>
 *   <li>对源集群来说，本组件等同于一个只读连接</li>
 * </ul>
 * <p>
 * <b>统一 ZMQ 通信模型：</b>
 * <ul>
 *   <li>Source 启动 ZMQ REP Socket 等待 Sink 的 PullRequest</li>
 *   <li>支持 --with-sink 参数在同进程内嵌启动 Sink 实例</li>
 *   <li>内嵌 Sink 通过 localhost:{zmqPort} 连接 Source，通信逻辑与独立部署完全一致</li>
 * </ul>
 * <p>
 * 启动流程：
 * <ol>
 *   <li>加载配置（三层合并）</li>
 *   <li>创建 HASource 实例（绑定 ZMQ REP Socket）</li>
 *   <li>如果指定 --with-sink，在同进程内嵌启动 Sink</li>
 *   <li>注册 JVM ShutdownHook 实现优雅停机</li>
 * </ol>
 */
public class SourceBootstrap {

    private static final Logger log = LoggerFactory.getLogger(SourceBootstrap.class);

    public static void main(String[] args) {
        log.info("=== RocketMQ HA Sync — Source 启动 ===");
        log.info("[约束] 本组件以虚拟 Slave 身份运行，不参与源集群选举和投票");
        log.info("[通信] 统一 ZMQ REQ-REP 模式，无 BlockingQueue 直连");

        // 1. 加载配置
        SourceConfig config = new SourceConfig();
        config.load(args);

        log.info("Source 配置加载完成");
        log.info("  源集群 NameServer: {}", config.getSourceNamesrv());
        log.info("  目标集群 NameServer: {}", config.getTargetNamesrv());
        log.info("  节点 ID: {}", config.getSourceNodeId());
        log.info("  ZMQ 绑定端口: {}", config.getZmqBindPort());
        log.info("  内嵌 Sink: {}", config.isWithSink());

        // 2. 创建 HASource（阶段二实现具体类）
        // TODO: 阶段二 — 创建 HASource 实例（绑定 ZMQ REP Socket）

        // 3. 如果指定 --with-sink，在同进程内嵌启动 Sink
        if (config.isWithSink()) {
            log.info("=== --with-sink 已启用，同进程内嵌启动 Sink ===");
            log.info("  内嵌 Sink 将通过 localhost:{} 连接 Source ZMQ Socket", config.getZmqBindPort());
            log.info("  通信协议和服务发现与独立部署完全一致");

            SinkConfig embeddedSinkConfig = SyncPipeline.buildEmbeddedSinkConfig(config);
            // TODO: 阶段四 — 创建 RocketMQSink 实例并启动
            // Sink 通过 ZMQ REQ 连接 localhost:{zmqPort}，行为与独立部署完全相同
        }

        // 4. 注册 ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("收到停机信号，开始优雅退出...");
            // TODO: 停止 Source 和内嵌 Sink
            log.info("Source 优雅退出完成");
        }, "source-shutdown-hook"));

        log.info("=== Source 已启动，等待数据同步 ===");
    }
}
