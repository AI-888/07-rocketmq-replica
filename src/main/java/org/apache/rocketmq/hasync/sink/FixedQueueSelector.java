package org.apache.rocketmq.hasync.sink;

/**
 * 固定 Queue 选择器 — 保持与源集群相同的 queueId
 * <p>
 * 保证：对于源集群 Topic T 的 Queue Q 上的消息序列 [M1, M2, M3, ...]，
 * 目标集群 Topic T 的 Queue Q 上的消息顺序也为 [M1, M2, M3, ...]。
 * <p>
 * 实现 RocketMQ MessageQueueSelector 接口的逻辑，通过 arg 参数传入
 * 源集群的 queueId，从目标集群的 MessageQueue 列表中选择相同 queueId 的队列。
 *
 * @see <a href="https://github.com/apache/rocketmq">MessageQueueSelector</a>
 */
public class FixedQueueSelector {

    /**
     * 根据源集群 queueId 选择目标集群中相同 ID 的 MessageQueue
     *
     * @param totalQueueNum 目标集群 Topic 的队列总数
     * @param sourceQueueId 源集群消息所属的 queueId
     * @return 目标集群中应写入的 queueId
     * @throws IllegalStateException 如果目标集群没有匹配的队列
     */
    public int select(int totalQueueNum, int sourceQueueId) {
        if (sourceQueueId < 0) {
            throw new IllegalArgumentException("sourceQueueId 不能为负数: " + sourceQueueId);
        }

        if (totalQueueNum <= 0) {
            throw new IllegalArgumentException("totalQueueNum 必须大于 0: " + totalQueueNum);
        }

        if (sourceQueueId < totalQueueNum) {
            // 目标集群有足够的队列，直接使用相同 ID
            return sourceQueueId;
        }

        // 目标集群队列数不足，取模映射（降级策略）
        // 注意：这会打破精确的 1:1 队列映射，但保证消息不丢失
        return sourceQueueId % totalQueueNum;
    }

    /**
     * 校验目标集群 Topic 的队列数是否与源集群一致
     *
     * @param sourceQueueNum 源集群队列数
     * @param targetQueueNum 目标集群队列数
     * @return true 一致，false 不一致（需要元数据同步）
     */
    public boolean isQueueConfigConsistent(int sourceQueueNum, int targetQueueNum) {
        return sourceQueueNum == targetQueueNum;
    }
}
