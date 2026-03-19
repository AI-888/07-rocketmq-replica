package org.apache.rocketmq.hasync.model;

/**
 * SyncRecord 消息类型枚举
 * <p>
 * 标识消息的特殊类型，用于 Sink 写入时选择不同的投递策略：
 * <ul>
 *   <li>NORMAL：普通消息 — 直接写入</li>
 *   <li>DELAY_MESSAGE：延迟消息（需求 21 §21.4）— 设置 delayTimeLevel</li>
 *   <li>TIMER_MESSAGE：定时消息（需求 21 §21.5）— 设置 __STARTDELIVERTIME</li>
 * </ul>
 */
public enum SyncRecordType {

    /** 普通消息 */
    NORMAL,

    /** 延迟消息（源自 SCHEDULE_TOPIC_XXXX） */
    DELAY_MESSAGE,

    /** 定时消息（源自 rmq_sys_wheel_timer 或带 __STARTDELIVERTIME 属性） */
    TIMER_MESSAGE
}
