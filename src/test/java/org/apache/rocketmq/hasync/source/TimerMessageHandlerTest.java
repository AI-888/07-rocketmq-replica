package org.apache.rocketmq.hasync.source;

import org.apache.rocketmq.hasync.model.SyncRecord;
import org.apache.rocketmq.hasync.model.SyncRecordType;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * TimerMessageHandler 单元测试（需求 21 §21.5）
 */
public class TimerMessageHandlerTest {

    private TimerMessageHandler handler;

    @Before
    public void setUp() {
        handler = new TimerMessageHandler();
    }

    @Test
    public void testIsTimerTopicMessage() {
        assertTrue(handler.isTimerTopicMessage("rmq_sys_wheel_timer"));
        assertFalse(handler.isTimerTopicMessage("OrderTopic"));
        assertFalse(handler.isTimerTopicMessage(null));
    }

    @Test
    public void testHasTimerProperty() {
        Map<String, String> props = new HashMap<>();
        props.put("__STARTDELIVERTIME", "1679284800000");
        assertTrue(handler.hasTimerProperty(props));
    }

    @Test
    public void testHasTimerProperty_NoProperty() {
        Map<String, String> props = new HashMap<>();
        props.put("OTHER", "value");
        assertFalse(handler.hasTimerProperty(props));
    }

    @Test
    public void testHasTimerProperty_NullProps() {
        assertFalse(handler.hasTimerProperty(null));
    }

    @Test
    public void testTransformTimerTopicMessage_Success() {
        long futureTime = System.currentTimeMillis() + 60000;
        SyncRecord record = createTimerTopicRecord("OrderTopic", "2", String.valueOf(futureTime));

        SyncRecord result = handler.transformTimerTopicMessage(record);

        assertNotNull(result);
        assertEquals("OrderTopic", result.getTopic());
        assertEquals(2, result.getQueueId());
        assertEquals(SyncRecordType.TIMER_MESSAGE, result.getSyncRecordType());
        assertEquals(futureTime, result.getDeliverTimeMs());
        assertEquals(1, handler.getSyncCount());
        assertEquals(0, handler.getExpiredCount());
    }

    @Test
    public void testTransformTimerTopicMessage_Expired() {
        long pastTime = System.currentTimeMillis() - 60000;
        SyncRecord record = createTimerTopicRecord("OrderTopic", "0", String.valueOf(pastTime));

        SyncRecord result = handler.transformTimerTopicMessage(record);

        assertNotNull(result);
        assertEquals(SyncRecordType.TIMER_MESSAGE, result.getSyncRecordType());
        assertEquals(1, handler.getExpiredCount()); // 过期计数 +1
    }

    @Test
    public void testTransformTimerTopicMessage_MissingRealTopic() {
        SyncRecord record = new SyncRecord();
        record.setTopic("rmq_sys_wheel_timer");
        record.setPhysicOffset(100);
        Map<String, String> props = new HashMap<>();
        props.put("REAL_QID", "0");
        props.put("__STARTDELIVERTIME", "1679284800000");
        record.setProperties(props);

        SyncRecord result = handler.transformTimerTopicMessage(record);

        assertNull("缺少 REAL_TOPIC 应返回 null", result);
        assertEquals(1, handler.getParseErrorCount());
    }

    @Test
    public void testTransformTimerTopicMessage_MissingDeliverTime() {
        SyncRecord record = new SyncRecord();
        record.setTopic("rmq_sys_wheel_timer");
        record.setPhysicOffset(100);
        Map<String, String> props = new HashMap<>();
        props.put("REAL_TOPIC", "OrderTopic");
        props.put("REAL_QID", "0");
        record.setProperties(props);

        SyncRecord result = handler.transformTimerTopicMessage(record);

        assertNull("缺少 __STARTDELIVERTIME 应返回 null", result);
        assertEquals(1, handler.getParseErrorCount());
    }

    @Test
    public void testTransformTimerTopicMessage_NullProperties() {
        SyncRecord record = new SyncRecord();
        record.setTopic("rmq_sys_wheel_timer");
        record.setPhysicOffset(100);
        record.setProperties(null);

        SyncRecord result = handler.transformTimerTopicMessage(record);

        assertNull(result);
        assertEquals(1, handler.getParseErrorCount());
    }

    @Test
    public void testTransformTimerTopicMessage_InvalidQueueId() {
        SyncRecord record = new SyncRecord();
        record.setTopic("rmq_sys_wheel_timer");
        record.setPhysicOffset(100);
        Map<String, String> props = new HashMap<>();
        props.put("REAL_TOPIC", "OrderTopic");
        props.put("REAL_QID", "invalid");
        props.put("__STARTDELIVERTIME", "1679284800000");
        record.setProperties(props);

        SyncRecord result = handler.transformTimerTopicMessage(record);

        assertNull(result);
        assertEquals(1, handler.getParseErrorCount());
    }

    @Test
    public void testTransformTimerTopicMessage_InvalidDeliverTime() {
        SyncRecord record = new SyncRecord();
        record.setTopic("rmq_sys_wheel_timer");
        record.setPhysicOffset(100);
        Map<String, String> props = new HashMap<>();
        props.put("REAL_TOPIC", "OrderTopic");
        props.put("REAL_QID", "0");
        props.put("__STARTDELIVERTIME", "not_a_number");
        record.setProperties(props);

        SyncRecord result = handler.transformTimerTopicMessage(record);

        assertNull(result);
        assertEquals(1, handler.getParseErrorCount());
    }

    @Test
    public void testMarkAsTimerMessage() {
        SyncRecord record = new SyncRecord();
        record.setTopic("OrderTopic");
        record.setPhysicOffset(1000);
        long futureTime = System.currentTimeMillis() + 60000;
        Map<String, String> props = new HashMap<>();
        props.put("__STARTDELIVERTIME", String.valueOf(futureTime));
        record.setProperties(props);

        SyncRecord result = handler.markAsTimerMessage(record);

        assertNotNull(result);
        assertEquals(SyncRecordType.TIMER_MESSAGE, result.getSyncRecordType());
        assertEquals(futureTime, result.getDeliverTimeMs());
        assertEquals("OrderTopic", result.getTopic()); // Topic 不变
        assertEquals(1, handler.getSyncCount());
    }

    @Test
    public void testMarkAsTimerMessage_NoTimerProperty() {
        SyncRecord record = new SyncRecord();
        record.setTopic("OrderTopic");
        record.setPhysicOffset(1000);
        record.setProperties(new HashMap<>());

        SyncRecord result = handler.markAsTimerMessage(record);

        assertNotNull(result);
        assertEquals(SyncRecordType.NORMAL, result.getSyncRecordType()); // 不修改
    }

    @Test
    public void testMarkAsTimerMessage_Expired() {
        SyncRecord record = new SyncRecord();
        record.setTopic("OrderTopic");
        record.setPhysicOffset(1000);
        long pastTime = System.currentTimeMillis() - 60000;
        Map<String, String> props = new HashMap<>();
        props.put("__STARTDELIVERTIME", String.valueOf(pastTime));
        record.setProperties(props);

        handler.markAsTimerMessage(record);

        assertEquals(1, handler.getExpiredCount());
    }

    @Test
    public void testConstants() {
        assertEquals("rmq_sys_wheel_timer", TimerMessageHandler.TIMER_TOPIC);
        assertEquals("__STARTDELIVERTIME", TimerMessageHandler.DELIVER_TIME_KEY);
        assertEquals("REAL_TOPIC", TimerMessageHandler.REAL_TOPIC_KEY);
        assertEquals("REAL_QID", TimerMessageHandler.REAL_QID_KEY);
    }

    // ==================== 辅助方法 ====================

    private SyncRecord createTimerTopicRecord(String realTopic, String realQid, String deliverTime) {
        SyncRecord record = new SyncRecord();
        record.setTopic("rmq_sys_wheel_timer");
        record.setPhysicOffset(2000);
        record.setQueueId(0);
        Map<String, String> props = new HashMap<>();
        props.put("REAL_TOPIC", realTopic);
        props.put("REAL_QID", realQid);
        props.put("__STARTDELIVERTIME", deliverTime);
        record.setProperties(props);
        return record;
    }
}
