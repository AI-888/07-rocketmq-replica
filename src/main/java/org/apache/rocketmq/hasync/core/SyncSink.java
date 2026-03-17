package org.apache.rocketmq.hasync.core;

import org.apache.rocketmq.hasync.model.SyncRecord;

/**
 * 数据写入接口 — 负责消费 SyncRecord 并写入目标存储
 * <p>
 * 默认实现：RocketMQSink（阶段四实现）
 * <ul>
 *   <li>严格按 {@link SyncRecord#getPhysicOffset()} 升序写入（需求 2 §6c）</li>
 *   <li>保持与源集群相同的 queueId（需求 2 §6d）</li>
 *   <li>写入成功后调用 CheckpointCoordinator.commitOffset() 推进位点</li>
 *   <li>写入失败时阻塞后续消息以保证顺序（需求 2 §6f）</li>
 * </ul>
 * <p>
 * 支持通过工厂方法注入不同实现（需求 2 §6 — 扩展性设计）
 * 
 * @see SyncRecord
 * @see CheckpointCoordinator
 */
public interface SyncSink {

    /**
     * 启动 Sink
     * <p>
     * 初始化 RocketMQ Producer、ZMQ 连接、恢复 Checkpoint 等
     *
     * @throws Exception 启动失败时抛出异常
     */
    void start() throws Exception;

    /**
     * 停止 Sink
     * <p>
     * 等待队列消息处理完成、刷写最后一次 Checkpoint、关闭连接
     */
    void stop();

    /**
     * 写入单条记录到目标存储
     * <p>
     * 必须严格按 physicOffset 升序逐条调用，
     * 当前消息写入成功并确认后才能写入下一条（需求 2 §6c）
     *
     * @param record 待写入的 SyncRecord
     * @throws Exception 写入失败时抛出异常
     */
    void write(SyncRecord record) throws Exception;

    /**
     * 批量发送时的刷写逻辑
     *
     * @throws Exception 刷写失败时抛出异常
     */
    void flush() throws Exception;
}
