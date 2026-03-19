package org.apache.rocketmq.hasync.source;

import org.apache.rocketmq.hasync.model.SyncRecord;
import org.apache.rocketmq.hasync.model.SyncRecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 定时消息处理器（需求 21 §21.5）
 * <p>
 * 在 CommitLogParser 中识别并解析定时消息：
 * <ul>
 *   <li>RocketMQ 5.x TimerWheel 系统 Topic：rmq_sys_wheel_timer</li>
 *   <li>带有 __STARTDELIVERTIME 属性的消息</li>
 * </ul>
 * <p>
 * 提取原始 Topic、QueueId 和定时投递时间戳，产出 TIMER_MESSAGE 类型的 SyncRecord。
 * Sink 写入时需在消息属性中设置 __STARTDELIVERTIME。
 */
public class TimerMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(TimerMessageHandler.class);

    /** RocketMQ 5.x TimerWheel 系统 Topic */
    public static final String TIMER_TOPIC = "rmq_sys_wheel_timer";

    /** 定时投递属性 key */
    public static final String DELIVER_TIME_KEY = "__STARTDELIVERTIME";

    /** 原始 Topic 属性 key */
    public static final String REAL_TOPIC_KEY = "REAL_TOPIC";

    /** 原始 QueueId 属性 key */
    public static final String REAL_QID_KEY = "REAL_QID";

    /** 已同步的定时消息计数 */
    private final AtomicLong syncCount = new AtomicLong(0);

    /** 已过期（立即投递）的定时消息计数 */
    private final AtomicLong expiredCount = new AtomicLong(0);

    /** 解析失败的定时消息计数 */
    private final AtomicLong parseErrorCount = new AtomicLong(0);

    /**
     * 判断是否为 TimerWheel 系统 Topic 消息
     *
     * @param topic 消息 Topic
     * @return true 如果是 rmq_sys_wheel_timer
     */
    public boolean isTimerTopicMessage(String topic) {
        return TIMER_TOPIC.equals(topic);
    }

    /**
     * 判断是否为带定时属性的消息
     *
     * @param properties 消息属性
     * @return true 如果包含 __STARTDELIVERTIME
     */
    public boolean hasTimerProperty(Map<String, String> properties) {
        return properties != null && properties.containsKey(DELIVER_TIME_KEY);
    }

    /**
     * 解析 TimerWheel Topic 中的定时消息
     * <p>
     * 从消息属性中提取原始 Topic、QueueId 和定时投递时间。
     *
     * @param record 原始解析出的 SyncRecord（topic=rmq_sys_wheel_timer）
     * @return 转换后的 SyncRecord（type=TIMER_MESSAGE），解析失败返回 null
     */
    public SyncRecord transformTimerTopicMessage(SyncRecord record) {
        Map<String, String> properties = record.getProperties();
        if (properties == null) {
            parseErrorCount.incrementAndGet();
            log.warn("定时消息解析失败，properties 为空：offset={}", record.getPhysicOffset());
            return null;
        }

        String realTopic = properties.get(REAL_TOPIC_KEY);
        String realQidStr = properties.get(REAL_QID_KEY);
        String deliverTimeStr = properties.get(DELIVER_TIME_KEY);

        if (realTopic == null || realQidStr == null || deliverTimeStr == null) {
            parseErrorCount.incrementAndGet();
            log.warn("定时消息解析失败，缺少必要属性：offset={}, REAL_TOPIC={}, REAL_QID={}, __STARTDELIVERTIME={}",
                    record.getPhysicOffset(), realTopic, realQidStr, deliverTimeStr);
            return null;
        }

        int realQueueId;
        long deliverTimeMs;
        try {
            realQueueId = Integer.parseInt(realQidStr);
        } catch (NumberFormatException e) {
            parseErrorCount.incrementAndGet();
            log.warn("定时消息解析失败，REAL_QID 非法：offset={}, REAL_QID={}",
                    record.getPhysicOffset(), realQidStr);
            return null;
        }
        try {
            deliverTimeMs = Long.parseLong(deliverTimeStr);
        } catch (NumberFormatException e) {
            parseErrorCount.incrementAndGet();
            log.warn("定时消息解析失败，__STARTDELIVERTIME 非法：offset={}, value={}",
                    record.getPhysicOffset(), deliverTimeStr);
            return null;
        }

        // 转换 SyncRecord
        record.setTopic(realTopic);
        record.setQueueId(realQueueId);
        record.setSyncRecordType(SyncRecordType.TIMER_MESSAGE);
        record.setDeliverTimeMs(deliverTimeMs);

        // 检查是否已过期
        if (deliverTimeMs <= System.currentTimeMillis()) {
            expiredCount.incrementAndGet();
            if (log.isDebugEnabled()) {
                log.debug("定时消息已过期，将立即投递：topic={}, originalDeliverTime={}",
                        realTopic, deliverTimeMs);
            }
        }

        syncCount.incrementAndGet();

        if (log.isDebugEnabled()) {
            log.debug("定时消息解析成功：offset={}, realTopic={}, realQueueId={}, deliverTimeMs={}",
                    record.getPhysicOffset(), realTopic, realQueueId, deliverTimeMs);
        }

        return record;
    }

    /**
     * 标记带有 __STARTDELIVERTIME 属性的普通消息为定时消息
     * <p>
     * 适用于 RocketMQ 4.x 风格的定时消息（属性直接在消息上，不经过 TimerWheel Topic）
     *
     * @param record 已解析的 SyncRecord
     * @return 标记后的 SyncRecord（type=TIMER_MESSAGE）
     */
    public SyncRecord markAsTimerMessage(SyncRecord record) {
        String deliverTimeStr = record.getProperties().get(DELIVER_TIME_KEY);
        if (deliverTimeStr == null) {
            return record; // 不修改
        }

        try {
            long deliverTimeMs = Long.parseLong(deliverTimeStr);
            record.setSyncRecordType(SyncRecordType.TIMER_MESSAGE);
            record.setDeliverTimeMs(deliverTimeMs);

            if (deliverTimeMs <= System.currentTimeMillis()) {
                expiredCount.incrementAndGet();
            }

            syncCount.incrementAndGet();
        } catch (NumberFormatException e) {
            log.warn("__STARTDELIVERTIME 属性非法，忽略定时标记：offset={}, value={}",
                    record.getPhysicOffset(), deliverTimeStr);
        }

        return record;
    }

    // ==================== Metrics Getters ====================

    public long getSyncCount() {
        return syncCount.get();
    }

    public long getExpiredCount() {
        return expiredCount.get();
    }

    public long getParseErrorCount() {
        return parseErrorCount.get();
    }
}
