package org.apache.rocketmq.hasync.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 监控指标采集器 — 统一聚合 Source / Sink / Pipeline 三维度指标
 * <p>
 * 对应需求 20（监控指标采集）：
 * <ul>
 *   <li>Source 侧指标：连接状态、重连次数、解析错误等</li>
 *   <li>Sink 侧指标：写入成功/失败、过滤、Checkpoint 等</li>
 *   <li>Pipeline 侧指标：吞吐量、队列积压、延迟等</li>
 * </ul>
 * <p>
 * 线程安全，所有计数器使用 {@link AtomicLong}，状态字段使用 volatile。
 */
public class MetricsCollector {

    // ==================== Source 侧指标 ====================

    /** 连接状态：CONNECTED / RECONNECTING / DISCONNECTED / PARSE_ERROR_SUSPENDED */
    private volatile String connectionStatus = "DISCONNECTED";

    /** 当前 Master HA 地址 */
    private volatile String currentMasterAddr = "";

    /** 连续重连失败持续时长（秒） */
    private volatile long continuousFailDurationSeconds = 0;

    /** TCP 连接断开累计次数 */
    private final AtomicLong connectionErrorCount = new AtomicLong(0);

    /** 重连 Master 累计次数 */
    private final AtomicLong retryCount = new AtomicLong(0);

    /** NameServer 查询失败累计次数 */
    private final AtomicLong nameSrvQueryErrorCount = new AtomicLong(0);

    /** 消息解析失败跳过的消息条数 */
    private final AtomicLong parseErrorCount = new AtomicLong(0);

    /** 半包丢弃累计次数 */
    private final AtomicLong halfPacketDropCount = new AtomicLong(0);

    /** 偏移量不一致累计次数 */
    private final AtomicLong offsetMismatchCount = new AtomicLong(0);

    /** Master 地址变更累计次数 */
    private final AtomicLong masterSwitchCount = new AtomicLong(0);

    /** 暂停状态：RUNNING / PARSE_ERROR_SUSPENDED */
    private volatile String parseErrorSuspendStatus = "RUNNING";

    /** 暂停持续时长（秒） */
    private volatile long parseErrorSuspendDurationSeconds = 0;

    /** 历史暂停触发累计次数 */
    private final AtomicLong parseErrorSuspendCount = new AtomicLong(0);

    // ==================== Sink 侧指标 ====================

    /** 成功写入目标集群的消息条数 */
    private final AtomicLong syncSuccessCount = new AtomicLong(0);

    /** 写入失败的消息条数 */
    private final AtomicLong syncFailureCount = new AtomicLong(0);

    /** Topic 过滤跳过的消息条数 */
    private final AtomicLong filteredMessageCount = new AtomicLong(0);

    /** 目标集群写入失败累计次数 */
    private final AtomicLong storageWriteErrorCount = new AtomicLong(0);

    /** Checkpoint 刷写失败累计次数 */
    private final AtomicLong checkpointFlushErrorCount = new AtomicLong(0);

    /** 启动一致性校验结果：PASSED / FAILED / SKIPPED */
    private volatile String startupCheckResult = "SKIPPED";

    /** 启动校验找到的消息条数 */
    private final AtomicLong startupCheckMsgFound = new AtomicLong(0);

    /** 目标集群状态：AVAILABLE / UNAVAILABLE */
    private volatile String targetClusterStatus = "AVAILABLE";

    /** 目标集群不可写持续时长（秒） */
    private volatile long targetUnavailableDurationSeconds = 0;

    /** 探活成功累计次数 */
    private final AtomicLong targetProbeSuccessCount = new AtomicLong(0);

    /** 探活失败累计次数 */
    private final AtomicLong targetProbeFailureCount = new AtomicLong(0);

    /** RFQ 发送成功条数 */
    private final AtomicLong rfqSendSuccessCount = new AtomicLong(0);

    /** RFQ 发送失败条数 */
    private final AtomicLong rfqSendFailureCount = new AtomicLong(0);

    /** RFQ 本地备用文件写入条数 */
    private final AtomicLong rfqFallbackCount = new AtomicLong(0);

    /** Topic 按需同步触发次数 */
    private final AtomicLong topicSyncOnDemandCount = new AtomicLong(0);

    /** Topic 同步失败次数 */
    private final AtomicLong topicSyncFailureCount = new AtomicLong(0);

    /** 是否因 Topic 同步失败暂停 */
    private volatile boolean topicSyncSuspended = false;

    /** 导致暂停的 Topic 名称 */
    private volatile String topicSyncFailedTopic = "";

    /** 当前生效的 Topic 过滤白名单（需求 20 §5：Sink 侧同时包含 activeTopicFilter） */
    private volatile String activeTopicFilter = "";

    // ==================== Pipeline 侧指标 ====================

    /** 每秒同步字节数 */
    private volatile long syncBytesPerSecond = 0;

    /** 队列当前积压数量 */
    private volatile int queueSize = 0;

    /** 已确认位点 */
    private volatile long confirmedOffset = 0;

    /** Master 最新偏移量 */
    private volatile long masterOffset = 0;

    /** 同步滞后字节数 */
    private volatile long lagBytes = 0;

    /** 最近 Checkpoint 刷写时间 */
    private volatile String lastCheckpointFlushTime = "";

    /** 元数据同步成功累计次数 */
    private final AtomicLong metaSyncSuccessCount = new AtomicLong(0);

    /** 元数据同步失败累计次数 */
    private final AtomicLong metaSyncErrorCount = new AtomicLong(0);

    /** 最近元数据同步成功时间 */
    private volatile String lastMetaSyncTime = "";

    /** 平均端到端延迟 (ms) */
    private volatile long avgEndToEndLatencyMs = 0;

    /** P99 端到端延迟 (ms) */
    private volatile long p99EndToEndLatencyMs = 0;

    /** 当前 TPS */
    private volatile long currentTps = 0;

    // ==================== Source 侧 increment 方法 ====================

    public void incrementConnectionErrorCount() { connectionErrorCount.incrementAndGet(); }
    public void incrementRetryCount() { retryCount.incrementAndGet(); }
    public void incrementNameSrvQueryErrorCount() { nameSrvQueryErrorCount.incrementAndGet(); }
    public void incrementParseErrorCount() { parseErrorCount.incrementAndGet(); }
    public void incrementHalfPacketDropCount() { halfPacketDropCount.incrementAndGet(); }
    public void incrementOffsetMismatchCount() { offsetMismatchCount.incrementAndGet(); }
    public void incrementMasterSwitchCount() { masterSwitchCount.incrementAndGet(); }
    public void incrementParseErrorSuspendCount() { parseErrorSuspendCount.incrementAndGet(); }

    // ==================== Sink 侧 increment 方法 ====================

    public void incrementSyncSuccessCount() { syncSuccessCount.incrementAndGet(); }
    public void incrementSyncFailureCount() { syncFailureCount.incrementAndGet(); }
    public void incrementFilteredMessageCount() { filteredMessageCount.incrementAndGet(); }
    public void incrementStorageWriteErrorCount() { storageWriteErrorCount.incrementAndGet(); }
    public void incrementCheckpointFlushErrorCount() { checkpointFlushErrorCount.incrementAndGet(); }
    public void incrementTargetProbeSuccessCount() { targetProbeSuccessCount.incrementAndGet(); }
    public void incrementTargetProbeFailureCount() { targetProbeFailureCount.incrementAndGet(); }
    public void incrementRfqSendSuccessCount() { rfqSendSuccessCount.incrementAndGet(); }
    public void incrementRfqSendFailureCount() { rfqSendFailureCount.incrementAndGet(); }
    public void incrementRfqFallbackCount() { rfqFallbackCount.incrementAndGet(); }
    public void incrementTopicSyncOnDemandCount() { topicSyncOnDemandCount.incrementAndGet(); }
    public void incrementTopicSyncFailureCount() { topicSyncFailureCount.incrementAndGet(); }

    // ==================== Pipeline 侧 increment 方法 ====================

    public void incrementMetaSyncSuccessCount() { metaSyncSuccessCount.incrementAndGet(); }
    public void incrementMetaSyncErrorCount() { metaSyncErrorCount.incrementAndGet(); }

    // ==================== Setters ====================

    public void setConnectionStatus(String status) { this.connectionStatus = status; }
    public void setCurrentMasterAddr(String addr) { this.currentMasterAddr = addr; }
    public void setContinuousFailDurationSeconds(long seconds) { this.continuousFailDurationSeconds = seconds; }
    public void setParseErrorSuspendStatus(String status) { this.parseErrorSuspendStatus = status; }
    public void setParseErrorSuspendDurationSeconds(long seconds) { this.parseErrorSuspendDurationSeconds = seconds; }
    public void setStartupCheckResult(String result) { this.startupCheckResult = result; }
    public void setStartupCheckMsgFound(long count) { this.startupCheckMsgFound.set(count); }
    public void setTargetClusterStatus(String status) { this.targetClusterStatus = status; }
    public void setTargetUnavailableDurationSeconds(long seconds) { this.targetUnavailableDurationSeconds = seconds; }
    public void setTopicSyncSuspended(boolean suspended, String topic) {
        this.topicSyncSuspended = suspended;
        this.topicSyncFailedTopic = suspended ? topic : "";
    }
    public void setActiveTopicFilter(String filter) { this.activeTopicFilter = filter; }
    public void setSyncBytesPerSecond(long bytes) { this.syncBytesPerSecond = bytes; }
    public void setQueueSize(int size) { this.queueSize = size; }
    public void setConfirmedOffset(long offset) { this.confirmedOffset = offset; }
    public void setMasterOffset(long offset) { this.masterOffset = offset; }
    public void setLagBytes(long bytes) { this.lagBytes = bytes; }
    public void setLastCheckpointFlushTime(String time) { this.lastCheckpointFlushTime = time; }
    public void setMetaSyncSuccessCount(long count) { this.metaSyncSuccessCount.set(count); }
    public void setMetaSyncErrorCount(long count) { this.metaSyncErrorCount.set(count); }
    public void setLastMetaSyncTime(String time) { this.lastMetaSyncTime = time; }
    public void setAvgEndToEndLatencyMs(long ms) { this.avgEndToEndLatencyMs = ms; }
    /** 别名方法，兼容 CheckpointCoordinatorImpl 调用 */
    public void incrementCheckpointFlushError() { checkpointFlushErrorCount.incrementAndGet(); }
    public void setP99EndToEndLatencyMs(long ms) { this.p99EndToEndLatencyMs = ms; }
    public void setCurrentTps(long tps) { this.currentTps = tps; }

    // ==================== Getters ====================

    public String getConnectionStatus() { return connectionStatus; }
    public String getCurrentMasterAddr() { return currentMasterAddr; }
    public long getContinuousFailDurationSeconds() { return continuousFailDurationSeconds; }
    public long getConnectionErrorCount() { return connectionErrorCount.get(); }
    public long getRetryCount() { return retryCount.get(); }
    public long getNameSrvQueryErrorCount() { return nameSrvQueryErrorCount.get(); }
    public long getParseErrorCount() { return parseErrorCount.get(); }
    public long getHalfPacketDropCount() { return halfPacketDropCount.get(); }
    public long getOffsetMismatchCount() { return offsetMismatchCount.get(); }
    public long getMasterSwitchCount() { return masterSwitchCount.get(); }
    public String getParseErrorSuspendStatus() { return parseErrorSuspendStatus; }
    public long getParseErrorSuspendDurationSeconds() { return parseErrorSuspendDurationSeconds; }
    public long getParseErrorSuspendCount() { return parseErrorSuspendCount.get(); }

    public long getSyncSuccessCount() { return syncSuccessCount.get(); }
    public long getSyncFailureCount() { return syncFailureCount.get(); }
    public long getFilteredMessageCount() { return filteredMessageCount.get(); }
    public long getStorageWriteErrorCount() { return storageWriteErrorCount.get(); }
    public long getCheckpointFlushErrorCount() { return checkpointFlushErrorCount.get(); }
    public String getStartupCheckResult() { return startupCheckResult; }
    public long getStartupCheckMsgFound() { return startupCheckMsgFound.get(); }
    public String getTargetClusterStatus() { return targetClusterStatus; }
    public long getTargetUnavailableDurationSeconds() { return targetUnavailableDurationSeconds; }
    public long getTargetProbeSuccessCount() { return targetProbeSuccessCount.get(); }
    public long getTargetProbeFailureCount() { return targetProbeFailureCount.get(); }
    public long getRfqSendSuccessCount() { return rfqSendSuccessCount.get(); }
    public long getRfqSendFailureCount() { return rfqSendFailureCount.get(); }
    public long getRfqFallbackCount() { return rfqFallbackCount.get(); }
    public long getTopicSyncOnDemandCount() { return topicSyncOnDemandCount.get(); }
    public long getTopicSyncFailureCount() { return topicSyncFailureCount.get(); }
    public boolean isTopicSyncSuspended() { return topicSyncSuspended; }
    public String getTopicSyncFailedTopic() { return topicSyncFailedTopic; }
    public String getActiveTopicFilter() { return activeTopicFilter; }

    public long getSyncBytesPerSecond() { return syncBytesPerSecond; }
    public int getQueueSize() { return queueSize; }
    public long getConfirmedOffset() { return confirmedOffset; }
    public long getMasterOffset() { return masterOffset; }
    public long getLagBytes() { return lagBytes; }
    public String getLastCheckpointFlushTime() { return lastCheckpointFlushTime; }
    public long getMetaSyncSuccessCount() { return metaSyncSuccessCount.get(); }
    public long getMetaSyncErrorCount() { return metaSyncErrorCount.get(); }
    public String getLastMetaSyncTime() { return lastMetaSyncTime; }
    public long getAvgEndToEndLatencyMs() { return avgEndToEndLatencyMs; }
    public long getP99EndToEndLatencyMs() { return p99EndToEndLatencyMs; }
    public long getCurrentTps() { return currentTps; }

    // ==================== 快照方法 ====================

    /**
     * 获取 Source 侧指标快照
     */
    public Map<String, Object> getSourceMetrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("connectionStatus", connectionStatus);
        m.put("currentMasterAddr", currentMasterAddr);
        m.put("continuousFailDurationSeconds", continuousFailDurationSeconds);
        m.put("connectionErrorCount", connectionErrorCount.get());
        m.put("retryCount", retryCount.get());
        m.put("nameSrvQueryErrorCount", nameSrvQueryErrorCount.get());
        m.put("parseErrorCount", parseErrorCount.get());
        m.put("halfPacketDropCount", halfPacketDropCount.get());
        m.put("offsetMismatchCount", offsetMismatchCount.get());
        m.put("masterSwitchCount", masterSwitchCount.get());
        m.put("parseErrorSuspendStatus", parseErrorSuspendStatus);
        m.put("parseErrorSuspendDurationSeconds", parseErrorSuspendDurationSeconds);
        m.put("parseErrorSuspendCount", parseErrorSuspendCount.get());
        return m;
    }

    /**
     * 获取 Sink 侧指标快照
     */
    public Map<String, Object> getSinkMetrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("syncSuccessCount", syncSuccessCount.get());
        m.put("syncFailureCount", syncFailureCount.get());
        m.put("filteredMessageCount", filteredMessageCount.get());
        m.put("storageWriteErrorCount", storageWriteErrorCount.get());
        m.put("checkpointFlushErrorCount", checkpointFlushErrorCount.get());
        m.put("startupCheckResult", startupCheckResult);
        m.put("startupCheckMsgFound", startupCheckMsgFound.get());
        m.put("targetClusterStatus", targetClusterStatus);
        m.put("targetUnavailableDurationSeconds", targetUnavailableDurationSeconds);
        m.put("targetProbeSuccessCount", targetProbeSuccessCount.get());
        m.put("targetProbeFailureCount", targetProbeFailureCount.get());
        m.put("rfqSendSuccessCount", rfqSendSuccessCount.get());
        m.put("rfqSendFailureCount", rfqSendFailureCount.get());
        m.put("rfqFallbackCount", rfqFallbackCount.get());
        m.put("topicSyncOnDemandCount", topicSyncOnDemandCount.get());
        m.put("topicSyncFailureCount", topicSyncFailureCount.get());
        m.put("topicSyncSuspended", topicSyncSuspended);
        m.put("topicSyncFailedTopic", topicSyncFailedTopic);
        m.put("activeTopicFilter", activeTopicFilter);
        return m;
    }

    /**
     * 获取 Pipeline 侧指标快照
     */
    public Map<String, Object> getPipelineMetrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("syncBytesPerSecond", syncBytesPerSecond);
        m.put("queueSize", queueSize);
        m.put("confirmedOffset", confirmedOffset);
        m.put("masterOffset", masterOffset);
        m.put("lagBytes", lagBytes);
        m.put("lastCheckpointFlushTime", lastCheckpointFlushTime);
        m.put("metaSyncSuccessCount", metaSyncSuccessCount.get());
        m.put("metaSyncErrorCount", metaSyncErrorCount.get());
        m.put("lastMetaSyncTime", lastMetaSyncTime);
        m.put("avgEndToEndLatencyMs", avgEndToEndLatencyMs);
        m.put("p99EndToEndLatencyMs", p99EndToEndLatencyMs);
        m.put("currentTps", currentTps);
        return m;
    }

    /**
     * 获取全部指标快照（用于 /metrics 接口和日志输出）
     */
    public Map<String, Object> getAllMetrics() {
        Map<String, Object> all = new LinkedHashMap<>();
        all.put("source", getSourceMetrics());
        all.put("sink", getSinkMetrics());
        all.put("pipeline", getPipelineMetrics());
        return all;
    }

    /**
     * 重置所有计数器（测试用）
     */
    public void reset() {
        connectionStatus = "DISCONNECTED";
        currentMasterAddr = "";
        continuousFailDurationSeconds = 0;
        connectionErrorCount.set(0);
        retryCount.set(0);
        nameSrvQueryErrorCount.set(0);
        parseErrorCount.set(0);
        halfPacketDropCount.set(0);
        offsetMismatchCount.set(0);
        masterSwitchCount.set(0);
        parseErrorSuspendStatus = "RUNNING";
        parseErrorSuspendDurationSeconds = 0;
        parseErrorSuspendCount.set(0);

        syncSuccessCount.set(0);
        syncFailureCount.set(0);
        filteredMessageCount.set(0);
        storageWriteErrorCount.set(0);
        checkpointFlushErrorCount.set(0);
        startupCheckResult = "SKIPPED";
        startupCheckMsgFound.set(0);
        targetClusterStatus = "AVAILABLE";
        targetUnavailableDurationSeconds = 0;
        targetProbeSuccessCount.set(0);
        targetProbeFailureCount.set(0);
        rfqSendSuccessCount.set(0);
        rfqSendFailureCount.set(0);
        rfqFallbackCount.set(0);
        topicSyncOnDemandCount.set(0);
        topicSyncFailureCount.set(0);
        topicSyncSuspended = false;
        topicSyncFailedTopic = "";
        activeTopicFilter = "";

        syncBytesPerSecond = 0;
        queueSize = 0;
        confirmedOffset = 0;
        masterOffset = 0;
        lagBytes = 0;
        lastCheckpointFlushTime = "";
        metaSyncSuccessCount.set(0);
        metaSyncErrorCount.set(0);
        lastMetaSyncTime = "";
        avgEndToEndLatencyMs = 0;
        p99EndToEndLatencyMs = 0;
        currentTps = 0;
    }
}
