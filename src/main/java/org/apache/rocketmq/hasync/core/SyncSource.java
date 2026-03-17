package org.apache.rocketmq.hasync.core;

/**
 * 数据源接口 — 负责从 Master 拉取数据并产出 SyncRecord
 * <p>
 * 具体实现：HASource（阶段二实现）
 * <ul>
 *   <li>通过 NameServer 发现 Master HA 地址</li>
 *   <li>使用 DefaultHAService Slave 协议与 Master 建立 TCP 连接</li>
 *   <li>持续接收并解析 CommitLog 数据包</li>
 *   <li>不执行 Topic 过滤和存储写入（属于 Sink 职责）</li>
 * </ul>
 * 
 * @see org.apache.rocketmq.hasync.model.SyncRecord
 */
public interface SyncSource {

    /**
     * 启动 Source
     * <p>
     * 初始化连接、注册 ZMQ 地址、恢复 Checkpoint 等
     *
     * @throws Exception 启动失败时抛出异常
     */
    void start() throws Exception;

    /**
     * 停止 Source
     * <p>
     * 关闭 TCP 连接、ZMQ Socket，从 NameServer KV 删除注册
     */
    void stop();

    /**
     * 检查 Source 是否正在运行
     *
     * @return true 表示运行中
     */
    boolean isRunning();

    /**
     * 拉取数据
     * <p>
     * 从 Master 接收数据包，解析消息封装为 SyncRecord，
     * 暂存到内部缓冲区，并响应 Sink 的 Pull 请求。
     */
    void poll();
}
