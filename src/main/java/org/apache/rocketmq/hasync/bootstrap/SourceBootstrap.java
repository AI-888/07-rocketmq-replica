package org.apache.rocketmq.hasync.bootstrap;

import org.apache.rocketmq.hasync.config.SourceConfig;
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
 * 启动流程：
 * <ol>
 *   <li>加载配置（三层合并）</li>
 *   <li>创建 HASource 实例（阶段二实现）</li>
 *   <li>创建 SyncPipeline 并启动</li>
 *   <li>注册 JVM ShutdownHook 实现优雅停机</li>
 * </ol>
 */
public class SourceBootstrap {

    private static final Logger log = LoggerFactory.getLogger(SourceBootstrap.class);

    public static void main(String[] args) {
        log.info("=== RocketMQ HA Sync — Source 启动 ===");
        log.info("[约束] 本组件以虚拟 Slave 身份运行，不参与源集群选举和投票");

        // 1. 加载配置
        SourceConfig config = new SourceConfig();
        config.load(args);

        log.info("Source 配置加载完成");
        log.info("  源集群 NameServer: {}", config.getSourceNamesrv());
        log.info("  目标集群 NameServer: {}", config.getTargetNamesrv());
        log.info("  节点 ID: {}", config.getSourceNodeId());

        // 2. 创建 HASource 和 SyncPipeline（阶段二实现具体类）
        // TODO: 阶段二 — 创建 HASource 实例
        // TODO: 阶段二 — 创建 SyncPipeline 并启动

        // 3. 注册 ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("收到停机信号，开始优雅退出...");
            // TODO: 阶段二 — pipeline.stopAll()
            log.info("Source 优雅退出完成");
        }, "source-shutdown-hook"));

        log.info("=== Source 已启动，等待数据同步 ===");
    }
}
