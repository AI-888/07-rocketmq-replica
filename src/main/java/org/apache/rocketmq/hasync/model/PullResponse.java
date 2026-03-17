package org.apache.rocketmq.hasync.model;

import java.io.Serializable;
import java.util.List;

/**
 * Source → Sink 的 ZMQ 拉取响应
 * <p>
 * records 严格按 {@link SyncRecord#getPhysicOffset()} 升序排列（需求 2 §6b）
 */
public class PullResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /** SyncRecord 列表（按 physicOffset 升序排列） */
    private List<SyncRecord> records;

    /** 当前最大偏移量 */
    private long maxOffset;

    /** 响应状态 */
    private ResponseStatus status;

    public PullResponse() {
    }

    public PullResponse(List<SyncRecord> records, long maxOffset) {
        this.records = records;
        this.maxOffset = maxOffset;
        this.status = ResponseStatus.SUCCESS;
    }

    public PullResponse(ResponseStatus status) {
        this.status = status;
    }

    // ==================== Getters & Setters ====================

    public List<SyncRecord> getRecords() {
        return records;
    }

    public void setRecords(List<SyncRecord> records) {
        this.records = records;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "PullResponse{" +
                "recordCount=" + (records != null ? records.size() : 0) +
                ", maxOffset=" + maxOffset +
                ", status=" + status +
                '}';
    }
}
