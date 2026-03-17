package org.apache.rocketmq.hasync.model;

import com.alibaba.fastjson2.JSON;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 解析失败消息记录（Replica Fail Queue）
 * <p>
 * 当 HASource 解析 CommitLog 消息失败时，将失败消息封装为此对象，
 * 写入源集群的 RFQ Topic。
 * <p>
 * 序列化为 JSON 后作为 RocketMQ 消息 Body 发送。
 *
 * @see org.apache.rocketmq.hasync.source.RfqSink
 */
public class ReplicaFailRecord {

    /** 消息的原始字节数组 */
    private byte[] rawBytes;

    /** 该消息所在数据包的起始物理偏移量 */
    private long masterPhyOffset;

    /** 该消息在数据包内的相对偏移量 */
    private int offsetInPacket;

    /** 解析失败的错误描述（枚举值） */
    private String errorReason;

    /** 解析失败的时间戳（ISO-8601） */
    private String failTimestamp;

    /** 来源集群的 NameServer 地址 */
    private String sourceCluster;

    public ReplicaFailRecord() {
    }

    public ReplicaFailRecord(byte[] rawBytes, long masterPhyOffset, int offsetInPacket,
                              String errorReason, String sourceCluster) {
        this.rawBytes = rawBytes;
        this.masterPhyOffset = masterPhyOffset;
        this.offsetInPacket = offsetInPacket;
        this.errorReason = errorReason;
        this.sourceCluster = sourceCluster;
        this.failTimestamp = DateTimeFormatter.ISO_INSTANT
                .format(Instant.now().atZone(ZoneId.systemDefault()));
    }

    /**
     * 序列化为 JSON 字节数组（作为 RocketMQ 消息 Body）
     */
    public byte[] toJsonBytes() {
        return JSON.toJSONBytes(this);
    }

    /**
     * 从 JSON 字节数组反序列化
     */
    public static ReplicaFailRecord fromJsonBytes(byte[] bytes) {
        return JSON.parseObject(bytes, ReplicaFailRecord.class);
    }

    // ==================== Getters / Setters ====================

    public byte[] getRawBytes() {
        return rawBytes;
    }

    public void setRawBytes(byte[] rawBytes) {
        this.rawBytes = rawBytes;
    }

    public long getMasterPhyOffset() {
        return masterPhyOffset;
    }

    public void setMasterPhyOffset(long masterPhyOffset) {
        this.masterPhyOffset = masterPhyOffset;
    }

    public int getOffsetInPacket() {
        return offsetInPacket;
    }

    public void setOffsetInPacket(int offsetInPacket) {
        this.offsetInPacket = offsetInPacket;
    }

    public String getErrorReason() {
        return errorReason;
    }

    public void setErrorReason(String errorReason) {
        this.errorReason = errorReason;
    }

    public String getFailTimestamp() {
        return failTimestamp;
    }

    public void setFailTimestamp(String failTimestamp) {
        this.failTimestamp = failTimestamp;
    }

    public String getSourceCluster() {
        return sourceCluster;
    }

    public void setSourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    @Override
    public String toString() {
        return "ReplicaFailRecord{" +
                "masterPhyOffset=" + masterPhyOffset +
                ", offsetInPacket=" + offsetInPacket +
                ", errorReason='" + errorReason + '\'' +
                ", failTimestamp='" + failTimestamp + '\'' +
                ", sourceCluster='" + sourceCluster + '\'' +
                ", rawBytesLen=" + (rawBytes != null ? rawBytes.length : 0) +
                '}';
    }
}
