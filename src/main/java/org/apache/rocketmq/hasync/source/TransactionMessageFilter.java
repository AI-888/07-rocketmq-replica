package org.apache.rocketmq.hasync.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务消息过滤器（需求 21 §21.8）
 * <p>
 * 同步组件**不支持**事务消息的同步。当 CommitLogParser 识别到事务消息时，
 * 自动跳过，避免将事务中间状态的消息（Half Message / Op Message）同步到目标集群。
 * <p>
 * 设计决策：事务消息依赖源集群本地事务状态机（事务回查机制），
 * 其状态与源集群 Broker 紧耦合，无法在目标集群还原。
 * 已提交的事务消息（写入真实 Topic 的消息）将作为普通消息正常同步。
 */
public class TransactionMessageFilter {

    private static final Logger log = LoggerFactory.getLogger(TransactionMessageFilter.class);

    /** RocketMQ 事务 Half 消息 Topic */
    public static final String TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC";

    /** RocketMQ 事务 Op 消息 Topic */
    public static final String TRANS_OP_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC";

    /** 跳过的事务消息计数器 */
    private final AtomicLong skipCount = new AtomicLong(0);

    /**
     * 判断消息是否需要跳过（事务中间状态消息一律跳过）
     *
     * @param topic 消息的 Topic
     * @return true=跳过（事务 Half/Op 消息），false=正常同步
     */
    public boolean shouldSkip(String topic) {
        if (TRANS_HALF_TOPIC.equals(topic) || TRANS_OP_TOPIC.equals(topic)) {
            skipCount.incrementAndGet();
            if (log.isDebugEnabled()) {
                log.debug("跳过事务消息：topic={}", topic);
            }
            return true;
        }
        return false;
    }

    /**
     * 获取跳过的事务消息总数
     */
    public long getSkipCount() {
        return skipCount.get();
    }
}
