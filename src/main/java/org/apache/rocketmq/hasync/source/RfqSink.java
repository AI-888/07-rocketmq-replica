package org.apache.rocketmq.hasync.source;

import org.apache.rocketmq.hasync.model.ReplicaFailRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RFQ（Replica Fail Queue）写入组件
 * <p>
 * 负责将解析失败的消息写入源集群的 RFQ Topic。
 * 发送失败时写入本地备用文件（./rfq-fallback.jsonl）。
 * <p>
 * 发送模式：同步发送（确保消息被 Broker 确认接收），
 * 指数退避重试（最多 rfqMaxRetry 次）。
 *
 * @see ReplicaFailRecord
 */
public class RfqSink {

    private static final Logger log = LoggerFactory.getLogger(RfqSink.class);

    /** 本地备用文件路径 */
    private static final String FALLBACK_FILE = "./rfq-fallback.jsonl";

    private final String rfqTopic;
    private final String rfqProducerGroup;
    private final int maxRetry;
    private final String sourceNamesrvAddr;
    private volatile boolean started = false;

    /** RFQ 发送回调（抽象实际的 Producer 调用） */
    private RfqSendCallback sendCallback;

    /** 监控计数器 */
    private final AtomicLong rfqSendSuccessCount = new AtomicLong(0);
    private final AtomicLong rfqSendFailureCount = new AtomicLong(0);
    private final AtomicLong rfqFallbackCount = new AtomicLong(0);

    /**
     * RFQ 发送回调接口
     */
    public interface RfqSendCallback {
        /**
         * 发送 RFQ 消息到源集群
         *
         * @param topic      RFQ Topic
         * @param body       消息 Body（JSON 序列化的 ReplicaFailRecord）
         * @param properties 消息属性
         * @throws Exception 发送失败
         */
        void send(String topic, byte[] body, java.util.Map<String, String> properties) throws Exception;
    }

    public RfqSink(String rfqTopic, String rfqProducerGroup, int maxRetry, String sourceNamesrvAddr) {
        this.rfqTopic = rfqTopic;
        this.rfqProducerGroup = rfqProducerGroup;
        this.maxRetry = maxRetry;
        this.sourceNamesrvAddr = sourceNamesrvAddr;
    }

    public void setSendCallback(RfqSendCallback sendCallback) {
        this.sendCallback = sendCallback;
    }

    /**
     * 启动 RFQ Sink
     */
    public void start() {
        started = true;
        log.info("RFQ Sink 已启动: topic={}, producerGroup={}, maxRetry={}",
                rfqTopic, rfqProducerGroup, maxRetry);
    }

    /**
     * 停止 RFQ Sink
     */
    public void stop() {
        started = false;
        log.info("RFQ Sink 已停止");
    }

    /**
     * 发送解析失败的消息到 RFQ Topic
     * <p>
     * 指数退避重试，重试均失败后写入本地备用文件。
     *
     * @param record 解析失败记录
     */
    public void sendToRfq(ReplicaFailRecord record) {
        if (sendCallback == null || !started) {
            log.warn("RFQ Sink 未就绪，写入本地备用文件");
            writeToFallbackFile(record);
            return;
        }

        byte[] body = record.toJsonBytes();

        // 构建消息属性
        java.util.Map<String, String> properties = new java.util.HashMap<>();
        properties.put("MASTER_PHY_OFFSET", String.valueOf(record.getMasterPhyOffset()));
        properties.put("ERROR_REASON", record.getErrorReason());
        properties.put("SOURCE_CLUSTER", record.getSourceCluster());
        properties.put("FAIL_TIMESTAMP", record.getFailTimestamp());

        // 指数退避重试
        long retryInterval = 500;
        for (int i = 0; i <= maxRetry; i++) {
            try {
                sendCallback.send(rfqTopic, body, properties);
                rfqSendSuccessCount.incrementAndGet();
                log.debug("RFQ 消息发送成功: offset={}, error={}",
                        record.getMasterPhyOffset(), record.getErrorReason());
                return;
            } catch (Exception e) {
                if (i < maxRetry) {
                    log.warn("RFQ 消息发送失败（第 {} 次），{}ms 后重试: {}",
                            i + 1, retryInterval, e.getMessage());
                    try {
                        Thread.sleep(retryInterval);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    retryInterval = Math.min(retryInterval * 2, 5000);
                }
            }
        }

        // 重试均失败，写入本地备用文件
        rfqSendFailureCount.incrementAndGet();
        log.error("RFQ 消息发送失败（已重试 {} 次），写入本地备用文件: {}", maxRetry, record);
        writeToFallbackFile(record);
    }

    /**
     * 写入本地备用文件
     */
    private void writeToFallbackFile(ReplicaFailRecord record) {
        try (FileOutputStream fos = new FileOutputStream(FALLBACK_FILE, true);
             OutputStreamWriter writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
            writer.write(new String(record.toJsonBytes(), StandardCharsets.UTF_8));
            writer.write("\n");
            rfqFallbackCount.incrementAndGet();
            log.info("RFQ 消息已写入备用文件: {}", FALLBACK_FILE);
        } catch (IOException e) {
            log.error("写入 RFQ 备用文件失败: {}", e.getMessage());
        }
    }

    /**
     * 作为 CommitLogParser.ParseFailureCallback 使用
     */
    public CommitLogParser.ParseFailureCallback asParseFailureCallback() {
        return (rawBytes, masterPhyOffset, offsetInPacket, errorReason) -> {
            ReplicaFailRecord record = new ReplicaFailRecord(
                    rawBytes, masterPhyOffset, offsetInPacket, errorReason, sourceNamesrvAddr);
            sendToRfq(record);
        };
    }

    // ==================== Getters ====================

    public boolean isStarted() {
        return started;
    }

    public long getRfqSendSuccessCount() {
        return rfqSendSuccessCount.get();
    }

    public long getRfqSendFailureCount() {
        return rfqSendFailureCount.get();
    }

    public long getRfqFallbackCount() {
        return rfqFallbackCount.get();
    }

    public String getRfqTopic() {
        return rfqTopic;
    }

    public String getRfqProducerGroup() {
        return rfqProducerGroup;
    }
}
