package org.apache.rocketmq.hasync.core;

/**
 * 位点协调器接口 — 管理同步位点的读取与推进
 * <p>
 * 核心原则（需求 10 §1）：
 * <ol>
 *   <li>先写数据到目标集群</li>
 *   <li>确认写入成功</li>
 *   <li>最后推进 Checkpoint</li>
 * </ol>
 * 确保 confirmedOffset 仅代表已成功写入目标集群的数据。
 * <p>
 * 状态存储：所有 Checkpoint 数据存储在目标集群 NameServer KV 中（需求 2 §11-12），
 * 实现完全无状态设计。
 * 
 * @see SyncSink
 */
public interface CheckpointCoordinator {

    /**
     * 获取已确认的位点
     * <p>
     * 返回 globalCheckpoint = min(所有 Sink 的 commitOffset)。
     * Source 使用此值上报 Master 的 slaveMaxOffset（需求 10 §5）。
     *
     * @return 已确认位点
     */
    long getConfirmedOffset();

    /**
     * Sink 写入成功后提交偏移量
     * <p>
     * 遵循"先写数据、确认成功、再推进位点"的原则（需求 10 §1）。
     *
     * @param sinkId Sink 节点 ID
     * @param offset 已成功写入的偏移量
     */
    void commitOffset(String sinkId, long offset);

    /**
     * 启动时从 NameServer KV 恢复位点
     *
     * @param sinkId Sink 节点 ID
     * @return 恢复的位点值，0 表示首次启动
     */
    long recoverCheckpoint(String sinkId);

    /**
     * 强制刷写所有位点到 NameServer KV
     * <p>
     * 优雅停机时调用（需求 17 §1）
     */
    void flush();
}
