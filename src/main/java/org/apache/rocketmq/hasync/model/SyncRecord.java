package org.apache.rocketmq.hasync.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Source 与 Sink 之间传递的数据单元
 * <p>
 * Sink 必须严格按 {@link #physicOffset} 升序写入目标集群。
 * <p>
 * 消息类型通过 {@link #syncRecordType} 标识（需求 21 §21.4/§21.5）：
 * <ul>
 *   <li>NORMAL — 普通消息</li>
 *   <li>DELAY_MESSAGE — 延迟消息（需要设置 delayTimeLevel）</li>
 *   <li>TIMER_MESSAGE — 定时消息（需要设置 deliverTimeMs）</li>
 * </ul>
 * 
 * @see org.apache.rocketmq.hasync.core.SyncSource
 * @see org.apache.rocketmq.hasync.core.SyncSink
 */
public class SyncRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 数据包起始偏移量 */
    private long masterPhyOffset;

    /** 数据包结束偏移量（masterPhyOffset + bodySize） */
    private long endOffset;

    /**
     * 消息物理偏移量 — 绝对顺序依据
     * <p>
     * Sink 必须严格按此字段升序写入目标集群（需求 2 §6c）
     */
    private long physicOffset;

    /** 消息 Topic */
    private String topic;

    /** 队列 ID — Sink 写入时保持与源集群相同（需求 2 §6d） */
    private int queueId;

    /** 消息体字节数组 */
    private byte[] body;

    /** 消息大小（字节） */
    private int msgSize;

    /** 存储时间戳 */
    private long storeTimestamp;

    /** 接收时间戳（Source 接收到的时间） */
    private long receiveTimestamp;

    /**
     * 全链路追踪 ID
     * <p>
     * 格式：{nodeId}-{masterPhyOffset}-{offsetInPacket}（需求 19 §1）
     */
    private String traceId;

    /** 消息属性（可选） */
    private Map<String, String> properties;

    /** 消息标志位 */
    private int flag;

    /** 系统标志位 */
    private int sysFlag;

    /** 队列偏移量 */
    private long queueOffset;

    /** 消息生成时间戳 */
    private long bornTimestamp;

    /** 重复消费次数 */
    private int reconsumeTimes;

    /**
     * 消息类型（需求 21 §21.4/§21.5）
     * <p>
     * 默认 NORMAL，延迟消息为 DELAY_MESSAGE，定时消息为 TIMER_MESSAGE
     */
    private SyncRecordType syncRecordType;

    /**
     * 延迟级别（仅 DELAY_MESSAGE 类型有效，需求 21 §21.4）
     * <p>
     * 对应 RocketMQ delayTimeLevel：1=1s, 2=5s, 3=10s, ...
     */
    private int delayTimeLevel;

    /**
     * 定时投递时间戳（仅 TIMER_MESSAGE 类型有效，需求 21 §21.5）
     * <p>
     * Unix 毫秒时间戳，对应 __STARTDELIVERTIME 属性
     */
    private long deliverTimeMs;

    public SyncRecord() {
        this.properties = new HashMap<>();
        this.syncRecordType = SyncRecordType.NORMAL;
    }

    // ==================== Getters & Setters ====================

    public long getMasterPhyOffset() {
        return masterPhyOffset;
    }

    public void setMasterPhyOffset(long masterPhyOffset) {
        this.masterPhyOffset = masterPhyOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public long getPhysicOffset() {
        return physicOffset;
    }

    public void setPhysicOffset(long physicOffset) {
        this.physicOffset = physicOffset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getReceiveTimestamp() {
        return receiveTimestamp;
    }

    public void setReceiveTimestamp(long receiveTimestamp) {
        this.receiveTimestamp = receiveTimestamp;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void putProperty(String key, String value) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(key, value);
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public SyncRecordType getSyncRecordType() {
        return syncRecordType;
    }

    public void setSyncRecordType(SyncRecordType syncRecordType) {
        this.syncRecordType = syncRecordType;
    }

    public int getDelayTimeLevel() {
        return delayTimeLevel;
    }

    public void setDelayTimeLevel(int delayTimeLevel) {
        this.delayTimeLevel = delayTimeLevel;
    }

    public long getDeliverTimeMs() {
        return deliverTimeMs;
    }

    public void setDeliverTimeMs(long deliverTimeMs) {
        this.deliverTimeMs = deliverTimeMs;
    }

    @Override
    public String toString() {
        return "SyncRecord{" +
                "masterPhyOffset=" + masterPhyOffset +
                ", endOffset=" + endOffset +
                ", physicOffset=" + physicOffset +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", msgSize=" + msgSize +
                ", traceId='" + traceId + '\'' +
                ", syncRecordType=" + syncRecordType +
                '}';
    }
}