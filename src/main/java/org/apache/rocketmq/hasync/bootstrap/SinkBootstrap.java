package org.apache.rocketmq.hasync.bootstrap;

import org.apache.rocketmq.hasync.checkpoint.CheckpointCoordinatorImpl;
import org.apache.rocketmq.hasync.config.SinkConfig;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.apache.rocketmq.hasync.sink.RocketMQSink;
import org.apache.rocketmq.hasync.sink.SinkRetryPolicy;
import org.apache.rocketmq.hasync.sink.SourceDiscovery;
import org.apache.rocketmq.hasync.sink.TopicFilter;
import org.apache.rocketmq.hasync.sink.TopicOnDemandSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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

    /** 默认 brokerName（用于 Checkpoint 命名空间） */
    private static final String DEFAULT_BROKER_NAME = "broker-a";

    public static void main(String[] args) {
        log.info("=== RocketMQ HA Sync — Sink 启动 ===");
        log.info("[通信] 通过 ZMQ REQ-REP 模式从 Source 拉取数据");

        // 1. 加载配置
        SinkConfig config = new SinkConfig();
        config.load(args);

        log.info("Sink 配置加载完成");
        log.info("  源集群 NameServer: {}", config.getSourceNamesrv());
        log.info("  目标集群 NameServer: {}", config.getTargetNamesrv());
        log.info("  Sink ID: {}", config.getSinkId());
        log.info("  批量大小: {}", config.getSinkBatchSize());
        log.info("  写入线程数: {}", config.getSinkThreads());

        // 2. 从 NameServer KV 发现 Source ZMQ 地址
        String sourceNamesrv = config.getSourceNamesrv();
        if (sourceNamesrv == null || sourceNamesrv.trim().isEmpty()) {
            log.error("独立 Sink 模式下必须指定 --sourceNamesrv 参数（用于发现 Source ZMQ 地址）");
            System.exit(1);
        }
        SourceDiscovery sourceDiscovery = new SourceDiscovery(sourceNamesrv);
        List<String> sourceAddresses = sourceDiscovery.discoverSourceAddresses();

        if (sourceAddresses.isEmpty()) {
            log.warn("未发现任何 Source ZMQ 地址，Sink 将等待 Source 注册后重试连接");
        } else {
            log.info("发现 {} 个 Source ZMQ 地址: {}", sourceAddresses.size(), sourceAddresses);
        }

        // 3. 创建 Sink 实例
        RocketMQSink sink = createSink(config);

        // 4. 启动 Sink
        try {
            sink.start();
            log.info("RocketMQSink 启动成功: sinkId={}", config.getSinkId());
        } catch (Exception e) {
            log.error("RocketMQSink 启动失败，进程退出", e);
            System.exit(1);
        }

        // 5. 启动 Sink 拉取和写入主循环
        Thread sinkPullThread = new Thread(() -> {
            log.info("Sink 拉取线程已启动（通过 ZMQ REQ 从 Source 拉取数据）");
            while (sink.isRunning()) {
                try {
                    // Sink 通过 ZMQ REQ 发送 PullRequest，接收 PullResponse
                    // 解析 SyncRecord 后调用 write() 写入目标集群
                    // 实际的 ZMQ 拉取逻辑在 Sink 内部处理
                    sink.flush();
                    Thread.sleep(10); // 避免空转
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    if (sink.isRunning()) {
                        log.error("Sink 拉取循环异常", e);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
            log.info("Sink 拉取线程已退出");
        }, "sink-pull-thread");
        sinkPullThread.setDaemon(true);
        sinkPullThread.start();

        // 6. 注册 ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("收到停机信号，开始优雅退出...");
            sink.stop();
            log.info("RocketMQSink 已停止");
            log.info("Sink 优雅退出完成");
        }, "sink-shutdown-hook"));

        log.info("=== Sink 已启动，等待数据写入 ===");
    }

    /**
     * 创建 RocketMQSink 实例及其全部依赖组件
     *
     * @param config Sink 配置
     * @return 完整配置的 RocketMQSink 实例
     */
    private static RocketMQSink createSink(SinkConfig config) {
        // 创建 MetricsCollector
        MetricsCollector metricsCollector = new MetricsCollector();

        // 创建 CheckpointCoordinator
        CheckpointCoordinatorImpl checkpointCoordinator = new CheckpointCoordinatorImpl(
                DEFAULT_BROKER_NAME,
                1000L,  // flushInterval
                100     // flushBatchSize
        );
        checkpointCoordinator.setMetricsCollector(metricsCollector);

        // 创建 TopicFilter（白名单为空 → 不过滤）
        TopicFilter topicFilter = TopicFilter.fromCommaSeparated(null);
        topicFilter.setMetricsCollector(metricsCollector);

        // 创建 TopicOnDemandSync
        TopicOnDemandSync topicOnDemandSync = new TopicOnDemandSync(config.getTopicSyncMaxRetry());
        topicOnDemandSync.setMetricsCollector(metricsCollector);

        // 创建 SinkRetryPolicy
        SinkRetryPolicy retryPolicy = new SinkRetryPolicy(config.getSinkMaxRetry());
        retryPolicy.setMetricsCollector(metricsCollector);

        // 组装 RocketMQSink
        return new RocketMQSink(config, topicFilter, topicOnDemandSync,
                retryPolicy, checkpointCoordinator, metricsCollector);
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

        // 创建并启动内嵌 Sink
        RocketMQSink sink = createSink(config);
        try {
            sink.start();
            log.info("内嵌 Sink 启动成功: sinkId={}", config.getSinkId());
        } catch (Exception e) {
            log.error("内嵌 Sink 启动失败", e);
            return;
        }

        // 启动内嵌 Sink 拉取线程
        Thread sinkThread = new Thread(() -> {
            log.info("内嵌 Sink 拉取线程已启动");
            while (sink.isRunning()) {
                try {
                    sink.flush();
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    if (sink.isRunning()) {
                        log.error("内嵌 Sink 处理异常", e);
                    }
                }
            }
            log.info("内嵌 Sink 拉取线程已退出");
        }, "embedded-sink-thread");
        sinkThread.setDaemon(true);
        sinkThread.start();

        log.info("=== 内嵌 Sink 已启动 ===");
    }
}
