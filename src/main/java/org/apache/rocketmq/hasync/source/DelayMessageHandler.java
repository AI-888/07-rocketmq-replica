package org.apache.rocketmq.hasync.source;

import org.apache.rocketmq.hasync.model.SyncRecord;
import org.apache.rocketmq.hasync.model.SyncRecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 延迟消息处理器（需求 21 §21.4）
 * <p>
 * 在 CommitLogParser 中识别并解析延迟消息（SCHEDULE_TOPIC_XXXX），
 * 提取原始 Topic、QueueId 和延迟级别，产出 DELAY_MESSAGE 类型的 SyncRecord。
 * <p>
 * Sink 写入时需根据 delayTimeLevel 设置 RocketMQ Producer 的延迟级别。
 */
public class DelayMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(DelayMessageHandler.class);

    /** RocketMQ 延迟消息内部 Topic */
    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";

    /** 原始 Topic 属性 key */
    public static final String REAL_TOPIC_KEY = "REAL_TOPIC";

    /** 原始 QueueId 属性 key */
    public static final String REAL_QID_KEY = "REAL_QID";

    /** 延迟级别属性 key */
    public static final String DELAY_KEY = "DELAY";

    /** 已同步的延迟消息计数 */
    private final AtomicLong syncCount = new AtomicLong(0);

    /** 解析失败的延迟消息计数 */
    private final AtomicLong parseErrorCount = new AtomicLong(0);

    /** 因 Topic 过滤跳过的延迟消息计数 */
    private final AtomicLong skipCount = new AtomicLong(0);

    /**
     * 判断是否为延迟消息
     *
     * @param topic 消息 Topic
     * @return true 如果是 SCHEDULE_TOPIC_XXXX
     */
    public boolean isDelayMessage(String topic) {
        return SCHEDULE_TOPIC.equals(topic);
    }

    /**
     * 解析延迟消息，从属性中提取原始投递信息
     * <p>
     * 将 SCHEDULE_TOPIC_XXXX 中的消息还原为原始 Topic + 延迟级别。
     *
     * @param record 原始解析出的 SyncRecord（topic=SCHEDULE_TOPIC_XXXX）
     * @return 转换后的 SyncRecord（type=DELAY_MESSAGE），解析失败返回 null
     */
    public SyncRecord transformDelayMessage(SyncRecord record) {
        Map<String, String> properties = record.getProperties();
        if (properties == null) {
            parseErrorCount.incrementAndGet();
            log.warn("延迟消息解析失败，properties 为空：offset={}", record.getPhysicOffset());
            return null;
        }

        String realTopic = properties.get(REAL_TOPIC_KEY);
        String realQidStr = properties.get(REAL_QID_KEY);

        if (realTopic == null || realQidStr == null) {
            parseErrorCount.incrementAndGet();
            log.warn("延迟消息解析失败，缺少必要属性：offset={}, REAL_TOPIC={}, REAL_QID={}",
                    record.getPhysicOffset(), realTopic, realQidStr);
            return null;
        }

        int realQueueId;
        try {
            realQueueId = Integer.parseInt(realQidStr);
        } catch (NumberFormatException e) {
            parseErrorCount.incrementAndGet();
            log.warn("延迟消息解析失败，REAL_QID 非法：offset={}, REAL_QID={}",
                    record.getPhysicOffset(), realQidStr);
            return null;
        }

        int delayTimeLevel = 0;
        String delayStr = properties.get(DELAY_KEY);
        if (delayStr != null) {
            try {
                delayTimeLevel = Integer.parseInt(delayStr);
            } catch (NumberFormatException e) {
                log.warn("延迟消息 DELAY 属性非法，使用默认值 0：offset={}, DELAY={}",
                        record.getPhysicOffset(), delayStr);
            }
        }

        // 转换 SyncRecord
        record.setTopic(realTopic);
        record.setQueueId(realQueueId);
        record.setSyncRecordType(SyncRecordType.DELAY_MESSAGE);
        record.setDelayTimeLevel(delayTimeLevel);

        syncCount.incrementAndGet();

        if (log.isDebugEnabled()) {
            log.debug("延迟消息解析成功：offset={}, realTopic={}, realQueueId={}, delayTimeLevel={}",
                    record.getPhysicOffset(), realTopic, realQueueId, delayTimeLevel);
        }

        return record;
    }

    /**
     * 增加跳过计数（因 Topic 过滤不同步）
     */
    public void incrementSkipCount() {
        skipCount.incrementAndGet();
    }

    // ==================== Metrics Getters ====================

    public long getSyncCount() {
        return syncCount.get();
    }

    public long getParseErrorCount() {
        return parseErrorCount.get();
    }

    public long getSkipCount() {
        return skipCount.get();
    }
}
