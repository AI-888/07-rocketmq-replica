package org.apache.rocketmq.hasync.source;

import org.apache.rocketmq.hasync.config.SourceConfig;
import org.apache.rocketmq.hasync.core.CheckpointCoordinator;
import org.apache.rocketmq.hasync.core.SyncSource;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HASource — SyncSource 的核心实现
 * <p>
 * 职责：
 * <ul>
 *   <li>通过 NameServer 发现 Master HA 地址（需求 4）</li>
 *   <li>执行兼容性预校验（需求 5）</li>
 *   <li>CommitLog 过期检测（需求 6）</li>
 *   <li>使用 DefaultHAService Slave 协议与 Master 建立 TCP 连接（需求 3）</li>
 *   <li>持续接收并解析 CommitLog 数据包（需求 7）</li>
 *   <li>统计每个 Topic 的消息字节数</li>
 *   <li>通过 ZMQ REP Socket 向 Sink 提供数据（需求 2 §7）</li>
 *   <li>将 ZMQ 地址注册到源集群 NameServer KV（需求 2 §8）</li>
 *   <li>检测 Master 变更并自动重连（需求 8）</li>
 *   <li>将解析失败的消息写入源集群 RFQ Topic（需求 13）</li>
 * </ul>
 * <p>
 * 不执行 Topic 过滤和存储写入（属于 Sink 职责）。
 */
public class HASource implements SyncSource {

    private static final Logger log = LoggerFactory.getLogger(HASource.class);

    /** 指数退避初始间隔（毫秒） */
    private static final long INITIAL_RETRY_INTERVAL = 1000;

    /** 指数退避最大间隔（毫秒） */
    private static final long MAX_RETRY_INTERVAL = 30000;

    // ==================== 配置 ====================

    private final SourceConfig config;

    // ==================== 核心组件 ====================

    private final HASourceConnection haConnection;
    private final CommitLogParser parser;
    private final MasterDiscovery masterDiscovery;
    private final SourceRegistry registry;
    private final RfqSink rfqSink;
    private final CheckpointCoordinator checkpointCoordinator;
    private final MetricsCollector metricsCollector;

    // ==================== 数据缓冲 ====================

    /** 本地内存缓冲区（Sink 通过 ZMQ 拉取） */
    private final List<SyncRecord> localBuffer = Collections.synchronizedList(new ArrayList<>());

    /** 缓冲区最大容量 */
    private static final int MAX_BUFFER_SIZE = 10000;

    // ==================== 状态 ====================

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean suspended = new AtomicBoolean(false);
    private final AtomicLong continuousFailStartTime = new AtomicLong(0);
    private final AtomicInteger retryCount = new AtomicInteger(0);
    private final AtomicInteger connectionErrorCount = new AtomicInteger(0);
    private final AtomicLong parseErrorWindowStart = new AtomicLong(0);
    private final AtomicInteger parseErrorWindowCount = new AtomicInteger(0);

    /** 后台定时任务调度器 */
    private ScheduledExecutorService scheduler;

    // ==================== CommitLog 过期检测回调 ====================

    /** CommitLog 过期检测回调接口 */
    private CommitLogExpiryCallback expiryCallback;

    /**
     * CommitLog 过期检测回调
     */
    public interface CommitLogExpiryCallback {
        /**
         * 查询 Master 的 CommitLog 范围
         *
         * @param masterBrokerAddr Master Broker 地址
         * @return [minPhyOffset, maxPhyOffset]
         */
        long[] getCommitLogRange(String masterBrokerAddr) throws Exception;
    }

    // ==================== 构造器 ====================

    public HASource(SourceConfig config,
                    CheckpointCoordinator checkpointCoordinator,
                    MetricsCollector metricsCollector) {
        this.config = config;
        this.checkpointCoordinator = checkpointCoordinator;
        this.metricsCollector = metricsCollector;

        this.haConnection = new HASourceConnection();
        this.parser = new CommitLogParser();
        this.masterDiscovery = new MasterDiscovery(
                config.getString("sourceNamesrv"),
                config.getString("brokerName"));

        String sourceNamesrv = config.getString("sourceNamesrv");
        String zmqHost = getLocalHost();
        int zmqPort = config.getInt("zmqBindPort");
        String sourceNodeId = config.getSourceNodeId();

        this.registry = new SourceRegistry(sourceNamesrv, sourceNodeId,
                zmqHost, zmqPort);

        this.rfqSink = new RfqSink(
                config.getString("rfqTopic"),
                config.getString("rfqProducerGroup"),
                config.getInt("rfqMaxRetry"),
                config.getString("sourceNamesrv"));

        // 设置解析失败回调 → RFQ
        this.parser.setFailureCallback(rfqSink.asParseFailureCallback());
    }

    /**
     * 构造器（用于测试注入）
     */
    public HASource(SourceConfig config,
                    HASourceConnection haConnection,
                    CommitLogParser parser,
                    MasterDiscovery masterDiscovery,
                    SourceRegistry registry,
                    RfqSink rfqSink,
                    CheckpointCoordinator checkpointCoordinator,
                    MetricsCollector metricsCollector) {
        this.config = config;
        this.haConnection = haConnection;
        this.parser = parser;
        this.masterDiscovery = masterDiscovery;
        this.registry = registry;
        this.rfqSink = rfqSink;
        this.checkpointCoordinator = checkpointCoordinator;
        this.metricsCollector = metricsCollector;
    }

    // ==================== 生命周期 ====================

    @Override
    public void start() throws Exception {
        log.info("========== HASource 启动 ==========");

        running.set(true);

        // 1. 通过 NameServer 查询 Master HA 地址
        String masterHaAddr = masterDiscovery.discoverMasterHaAddr();
        metricsCollector.setCurrentMasterAddr(masterHaAddr);
        metricsCollector.setConnectionStatus("RECONNECTING");

        // 2. 建立 TCP 连接（仅使用 DefaultHAService 基础 Slave 协议）
        connectToMaster(masterHaAddr);

        // 3. 启动 RFQ Sink
        rfqSink.start();

        // 4. 执行兼容性预校验（需求 5）
        performCompatibilityCheck();

        // 5. CommitLog 过期检测（需求 6）
        checkCommitLogExpiry();

        // 6. 注册到源集群 NameServer KV（需求 2 §8）
        try {
            registry.register();
        } catch (Exception e) {
            log.warn("Source 注册失败（不阻止启动）: {}", e.getMessage());
        }

        // 7. 从 globalCheckpoint 恢复位点
        long checkpoint = checkpointCoordinator.getConfirmedOffset();
        if (checkpoint > 0) {
            haConnection.reportSlaveMaxOffset(checkpoint);
            log.info("从 Checkpoint 恢复，初始 slaveMaxOffset={}", checkpoint);
        }

        // 8. 启动后台定时任务
        startScheduledTasks();

        metricsCollector.setConnectionStatus("CONNECTED");
        log.info("========== HASource 启动完成 ==========");
    }

    @Override
    public void stop() {
        log.info("========== HASource 停止 ==========");
        running.set(false);

        // 1. 关闭 TCP 连接
        haConnection.close();

        // 2. 关闭 RFQ Sink
        rfqSink.stop();

        // 3. 注销 NameServer KV
        registry.unregister();

        // 4. 关闭定时任务
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        metricsCollector.setConnectionStatus("DISCONNECTED");
        log.info("========== HASource 已停止 ==========");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    // ==================== 数据拉取 ====================

    @Override
    public void poll() {
        if (!running.get()) return;

        // 解析失败暂停状态下不拉取新数据（需求 14 §13）
        if (suspended.get()) {
            metricsCollector.setParseErrorSuspendStatus("PARSE_ERROR_SUSPENDED");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return;
        }

        try {
            // 1. 从 TCP 连接接收 Master 数据包
            HASourceConnection.HADataPacket packet = haConnection.receive();
            if (packet == null) {
                return; // 心跳包
            }

            // 2. 解析消息
            List<SyncRecord> records = parser.parse(packet.getBody(), packet.getMasterPhyOffset());

            // 3. 检查解析失败暂停（需求 14 §12）
            checkParseErrorSuspend();

            // 4. 暂存到本地缓冲区
            for (SyncRecord record : records) {
                // 生成 Trace ID（需求 19 §1）
                if (record.getTraceId() == null) {
                    record.setTraceId(generateTraceId(record));
                }

                // 缓冲区满时阻塞
                while (localBuffer.size() >= MAX_BUFFER_SIZE && running.get()) {
                    Thread.sleep(10);
                }

                if (running.get()) {
                    localBuffer.add(record);
                    metricsCollector.incrementSyncSuccessCount();
                }
            }

            // 5. 更新指标
            metricsCollector.setMasterOffset(packet.getMasterPhyOffset() + packet.getBodySize());
            metricsCollector.setQueueSize(localBuffer.size());

            // 6. 上报 slaveMaxOffset（confirmedOffset）
            long confirmed = checkpointCoordinator.getConfirmedOffset();
            haConnection.reportSlaveMaxOffset(confirmed);

            // 重置连续失败计时
            continuousFailStartTime.set(0);
            retryCount.set(0);

        } catch (IOException e) {
            // 网络异常 → 触发重连
            connectionErrorCount.incrementAndGet();
            metricsCollector.incrementConnectionErrorCount();
            metricsCollector.setConnectionStatus("RECONNECTING");

            log.warn("Master 连接断开: {}", e.getMessage());
            handleReconnect();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("HASource poll 异常: {}", e.getMessage(), e);
        }
    }

    // ==================== 兼容性预校验（需求 5） ====================

    /**
     * 执行兼容性预校验
     */
    void performCompatibilityCheck() {
        try {
            // 尝试从 Master 拉取少量数据并解析
            HASourceConnection.HADataPacket packet = haConnection.receive();
            if (packet != null && packet.getBody() != null) {
                parser.compatibilityCheck(packet.getBody(), packet.getMasterPhyOffset());
                log.info("兼容性预校验成功，消息格式兼容，准备开始同步");
            } else {
                log.warn("Master CommitLog 为空，跳过兼容性预校验");
            }
        } catch (CommitLogParser.CommitLogIncompatibleException e) {
            log.error("消息格式不兼容，任务失败: {}", e.getMessage());
            running.set(false);
            throw e;
        } catch (IOException e) {
            log.warn("兼容性预校验时读取数据失败（将在正式同步时重试）: {}", e.getMessage());
        }
    }

    // ==================== CommitLog 过期检测（需求 6） ====================

    /**
     * CommitLog 过期检测
     */
    void checkCommitLogExpiry() {
        if (expiryCallback == null) {
            log.debug("CommitLog 过期检测回调未设置，跳过");
            return;
        }

        try {
            String masterBrokerAddr = masterDiscovery.getCurrentMasterBrokerAddr();
            if (masterBrokerAddr == null) {
                log.warn("Master Broker 地址未知，跳过 CommitLog 过期检测");
                return;
            }

            long[] range = expiryCallback.getCommitLogRange(masterBrokerAddr);
            long minPhyOffset = range[0];
            long maxPhyOffset = range[1];

            long confirmedOffset = checkpointCoordinator.getConfirmedOffset();

            if (confirmedOffset > 0 && confirmedOffset < minPhyOffset) {
                log.error("Checkpoint offset 已过期！Master 最小可用偏移量={}, 当前 Checkpoint={}，存在数据丢失风险",
                        minPhyOffset, confirmedOffset);

                // 检查是否允许强制重置
                String resetFlag = System.getenv("HA_SOURCE_RESET_TO_EARLIEST");
                if ("true".equalsIgnoreCase(resetFlag)) {
                    log.warn("将从 earliest offset 重新同步，可能产生重复数据。新 confirmedOffset={}",
                            minPhyOffset);
                    checkpointCoordinator.commitOffset("source", minPhyOffset);
                } else {
                    log.error("Checkpoint 过期，阻塞启动。设置环境变量 HA_SOURCE_RESET_TO_EARLIEST=true 可强制从最早偏移量重新同步");
                    throw new RuntimeException("Checkpoint offset 已过期，请设置 HA_SOURCE_RESET_TO_EARLIEST=true 或手动处理");
                }
            } else {
                log.info("CommitLog 过期检测通过: confirmedOffset={}, minPhyOffset={}, maxPhyOffset={}",
                        confirmedOffset, minPhyOffset, maxPhyOffset);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            log.warn("CommitLog 过期检测失败: {}", e.getMessage());
        }
    }

    // ==================== Master 切换重连（需求 8） ====================

    /**
     * 处理重连逻辑（指数退避）
     */
    void handleReconnect() {
        if (!running.get()) return;

        long retryInterval = INITIAL_RETRY_INTERVAL;

        while (running.get()) {
            try {
                // 重新发现 Master
                String newHaAddr = masterDiscovery.discoverMasterHaAddr(3);
                connectToMaster(newHaAddr);

                // 从 Checkpoint 恢复位点
                long confirmed = checkpointCoordinator.getConfirmedOffset();
                haConnection.reportSlaveMaxOffset(confirmed);

                metricsCollector.setConnectionStatus("CONNECTED");
                metricsCollector.setCurrentMasterAddr(newHaAddr);
                continuousFailStartTime.set(0);

                log.info("重连成功: masterAddr={}, confirmedOffset={}", newHaAddr, confirmed);
                return;

            } catch (Exception e) {
                retryCount.incrementAndGet();
                metricsCollector.incrementRetryCount();

                if (continuousFailStartTime.get() == 0) {
                    continuousFailStartTime.set(System.currentTimeMillis());
                }

                long failDuration = (System.currentTimeMillis() - continuousFailStartTime.get()) / 1000;
                metricsCollector.setContinuousFailDurationSeconds(failDuration);

                if (failDuration > 600) {
                    log.error("连续重连失败已超过 10 分钟（{}s），继续尝试...", failDuration);
                }

                log.warn("重连失败（第 {} 次），{}ms 后重试: {}",
                        retryCount.get(), retryInterval, e.getMessage());

                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                retryInterval = Math.min(retryInterval * 2, MAX_RETRY_INTERVAL);
            }
        }
    }

    /**
     * 连接到 Master
     */
    private void connectToMaster(String masterHaAddr) throws IOException {
        haConnection.connect(masterHaAddr);
    }

    // ==================== 解析失败暂停（需求 14 §12-16） ====================

    /**
     * 检查解析失败滑动窗口，必要时触发暂停
     */
    private void checkParseErrorSuspend() {
        long windowMs = config.getInt("parseErrorSuspendWindowMs");
        int threshold = getParseErrorSuspendThreshold();

        if (threshold <= 0) return; // 禁用暂停功能

        long now = System.currentTimeMillis();
        if (now - parseErrorWindowStart.get() > windowMs) {
            parseErrorWindowStart.set(now);
            parseErrorWindowCount.set(0);
        }

        long currentParseErrors = metricsCollector.getParseErrorCount();
        if (currentParseErrors > threshold) {
            suspended.set(true);
            metricsCollector.setParseErrorSuspendStatus("PARSE_ERROR_SUSPENDED");
            metricsCollector.incrementParseErrorSuspendCount();
            log.error("解析失败次数超过阈值（{}），暂停数据拉取。可通过 POST /resume 恢复",
                    threshold);
        }
    }

    /**
     * 恢复暂停状态
     */
    public void resume() {
        if (suspended.compareAndSet(true, false)) {
            parseErrorWindowCount.set(0);
            parseErrorWindowStart.set(System.currentTimeMillis());
            metricsCollector.setParseErrorSuspendStatus("RUNNING");
            log.info("已从暂停状态恢复，继续同步");
        }
    }

    private int getParseErrorSuspendThreshold() {
        String envVal = System.getenv("HA_SOURCE_PARSE_ERROR_SUSPEND_THRESHOLD");
        if (envVal != null) {
            try {
                return Integer.parseInt(envVal);
            } catch (NumberFormatException e) {
                // 忽略
            }
        }
        return 100; // 默认阈值
    }

    // ==================== 后台定时任务 ====================

    private void startScheduledTasks() {
        scheduler = new ScheduledThreadPoolExecutor(3, r -> {
            Thread t = new Thread(r, "ha-source-scheduler");
            t.setDaemon(true);
            return t;
        });

        // Master 变更检测（默认每 30 秒）
        long masterPollInterval = config.getInt("masterPollInterval");
        scheduler.scheduleAtFixedRate(this::pollMasterChange,
                masterPollInterval, masterPollInterval, TimeUnit.MILLISECONDS);

        // 统计刷新（每 10 秒）
        scheduler.scheduleAtFixedRate(this::refreshStats,
                10000, 10000, TimeUnit.MILLISECONDS);
    }

    /**
     * 主动轮询 Master 变更
     */
    private void pollMasterChange() {
        try {
            if (masterDiscovery.checkMasterChanged()) {
                log.info("检测到 Master 变更，触发主动重连");
                metricsCollector.incrementMasterSwitchCount();
                handleReconnect();
            }
        } catch (Exception e) {
            log.warn("Master 变更检测失败: {}", e.getMessage());
        }
    }

    /**
     * 刷新统计信息
     */
    private void refreshStats() {
        Map<String, AtomicLong> stats = parser.getTopicBytesStats();
        if (!stats.isEmpty()) {
            StringBuilder sb = new StringBuilder("Topic 流量统计: ");
            stats.forEach((topic, bytes) -> sb.append(topic).append("=")
                    .append(bytes.get()).append("B, "));
            log.info(sb.toString());
        }
    }

    // ==================== 数据访问（供 ZMQ 服务使用） ====================

    /**
     * 获取从指定 offset 开始的数据批次
     */
    public List<SyncRecord> getRecordsFrom(long fromOffset, int batchSize) {
        List<SyncRecord> result = new ArrayList<>();
        synchronized (localBuffer) {
            for (SyncRecord r : localBuffer) {
                if (r.getPhysicOffset() >= fromOffset) {
                    result.add(r);
                    if (result.size() >= batchSize) break;
                }
            }
        }
        return result;
    }

    /**
     * 清理已确认的缓冲区数据
     */
    public void cleanBuffer(long confirmedOffset) {
        synchronized (localBuffer) {
            localBuffer.removeIf(r -> r.getPhysicOffset() < confirmedOffset);
        }
    }

    // ==================== 辅助方法 ====================

    private String generateTraceId(SyncRecord record) {
        String nodeId = config.getString("sourceNodeId");
        return nodeId + "-" + record.getMasterPhyOffset() + "-" + record.getPhysicOffset();
    }

    private String getLocalHost() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    // ==================== Getters ====================

    public HASourceConnection getHaConnection() {
        return haConnection;
    }

    public CommitLogParser getParser() {
        return parser;
    }

    public MasterDiscovery getMasterDiscovery() {
        return masterDiscovery;
    }

    public SourceRegistry getRegistry() {
        return registry;
    }

    public RfqSink getRfqSink() {
        return rfqSink;
    }

    public boolean isSuspended() {
        return suspended.get();
    }

    public int getRetryCount() {
        return retryCount.get();
    }

    public int getConnectionErrorCount() {
        return connectionErrorCount.get();
    }

    public List<SyncRecord> getLocalBuffer() {
        return localBuffer;
    }

    public void setExpiryCallback(CommitLogExpiryCallback expiryCallback) {
        this.expiryCallback = expiryCallback;
    }
}