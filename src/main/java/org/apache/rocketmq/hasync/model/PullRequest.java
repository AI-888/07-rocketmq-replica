package org.apache.rocketmq.hasync.model;

import java.io.Serializable;
import java.util.Set;

/**
 * Sink → Source 的 ZMQ 拉取请求
 * <p>
 * Sink 通过 ZMQ REQ 发送此请求到 Source 的 ZMQ REP Socket（需求 2 §10）
 */
public class PullRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Topic 过滤白名单（可选，空或 null 表示不过滤） */
    private Set<String> topicFilter;

    /** 起始偏移量（从该偏移量开始拉取） */
    private long fromOffset;

    /** 批量大小（期望拉取的最大消息条数） */
    private int batchSize;

    /** Sink 节点 ID */
    private String sinkId;

    public PullRequest() {
    }

    public PullRequest(long fromOffset, int batchSize, String sinkId) {
        this.fromOffset = fromOffset;
        this.batchSize = batchSize;
        this.sinkId = sinkId;
    }

    // ==================== Getters & Setters ====================

    public Set<String> getTopicFilter() {
        return topicFilter;
    }

    public void setTopicFilter(Set<String> topicFilter) {
        this.topicFilter = topicFilter;
    }

    public long getFromOffset() {
        return fromOffset;
    }

    public void setFromOffset(long fromOffset) {
        this.fromOffset = fromOffset;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getSinkId() {
        return sinkId;
    }

    public void setSinkId(String sinkId) {
        this.sinkId = sinkId;
    }

    @Override
    public String toString() {
        return "PullRequest{" +
                "fromOffset=" + fromOffset +
                ", batchSize=" + batchSize +
                ", sinkId='" + sinkId + '\'' +
                ", topicFilter=" + topicFilter +
                '}';
    }
}
