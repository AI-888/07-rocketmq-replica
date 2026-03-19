package org.apache.rocketmq.hasync.source;

import org.apache.rocketmq.hasync.model.SyncRecord;
import org.apache.rocketmq.hasync.model.SyncRecordType;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * DelayMessageHandler 单元测试（需求 21 §21.4）
 */
public class DelayMessageHandlerTest {

    private DelayMessageHandler handler;

    @Before
    public void setUp() {
        handler = new DelayMessageHandler();
    }

    @Test
    public void testIsDelayMessage() {
        assertTrue(handler.isDelayMessage("SCHEDULE_TOPIC_XXXX"));
        assertFalse(handler.isDelayMessage("OrderTopic"));
        assertFalse(handler.isDelayMessage(null));
    }

    @Test
    public void testTransformDelayMessage_Success() {
        SyncRecord record = createScheduleTopicRecord("OrderTopic", "3", "5");

        SyncRecord result = handler.transformDelayMessage(record);

        assertNotNull("结果不应为 null", result);
        assertEquals("OrderTopic", result.getTopic());
        assertEquals(3, result.getQueueId());
        assertEquals(SyncRecordType.DELAY_MESSAGE, result.getSyncRecordType());
        assertEquals(5, result.getDelayTimeLevel());
        assertEquals(1, handler.getSyncCount());
        assertEquals(0, handler.getParseErrorCount());
    }

    @Test
    public void testTransformDelayMessage_NoDelayProperty() {
        SyncRecord record = createScheduleTopicRecord("OrderTopic", "0", null);

        SyncRecord result = handler.transformDelayMessage(record);

        assertNotNull("即使无 DELAY 属性也应成功", result);
        assertEquals("OrderTopic", result.getTopic());
        assertEquals(0, result.getDelayTimeLevel()); // 默认 0
    }

    @Test
    public void testTransformDelayMessage_MissingRealTopic() {
        SyncRecord record = new SyncRecord();
        record.setTopic("SCHEDULE_TOPIC_XXXX");
        record.setPhysicOffset(100);
        Map<String, String> props = new HashMap<>();
        props.put("REAL_QID", "0");
        record.setProperties(props);

        SyncRecord result = handler.transformDelayMessage(record);

        assertNull("缺少 REAL_TOPIC 应返回 null", result);
        assertEquals(1, handler.getParseErrorCount());
    }

    @Test
    public void testTransformDelayMessage_MissingRealQid() {
        SyncRecord record = new SyncRecord();
        record.setTopic("SCHEDULE_TOPIC_XXXX");
        record.setPhysicOffset(100);
        Map<String, String> props = new HashMap<>();
        props.put("REAL_TOPIC", "OrderTopic");
        record.setProperties(props);

        SyncRecord result = handler.transformDelayMessage(record);

        assertNull("缺少 REAL_QID 应返回 null", result);
        assertEquals(1, handler.getParseErrorCount());
    }

    @Test
    public void testTransformDelayMessage_NullProperties() {
        SyncRecord record = new SyncRecord();
        record.setTopic("SCHEDULE_TOPIC_XXXX");
        record.setPhysicOffset(100);
        record.setProperties(null);

        SyncRecord result = handler.transformDelayMessage(record);

        assertNull("null properties 应返回 null", result);
        assertEquals(1, handler.getParseErrorCount());
    }

    @Test
    public void testTransformDelayMessage_InvalidQueueId() {
        SyncRecord record = new SyncRecord();
        record.setTopic("SCHEDULE_TOPIC_XXXX");
        record.setPhysicOffset(100);
        Map<String, String> props = new HashMap<>();
        props.put("REAL_TOPIC", "OrderTopic");
        props.put("REAL_QID", "not_a_number");
        record.setProperties(props);

        SyncRecord result = handler.transformDelayMessage(record);

        assertNull("非法 REAL_QID 应返回 null", result);
        assertEquals(1, handler.getParseErrorCount());
    }

    @Test
    public void testIncrementSkipCount() {
        handler.incrementSkipCount();
        handler.incrementSkipCount();
        assertEquals(2, handler.getSkipCount());
    }

    @Test
    public void testConstants() {
        assertEquals("SCHEDULE_TOPIC_XXXX", DelayMessageHandler.SCHEDULE_TOPIC);
        assertEquals("REAL_TOPIC", DelayMessageHandler.REAL_TOPIC_KEY);
        assertEquals("REAL_QID", DelayMessageHandler.REAL_QID_KEY);
        assertEquals("DELAY", DelayMessageHandler.DELAY_KEY);
    }

    // ==================== 辅助方法 ====================

    private SyncRecord createScheduleTopicRecord(String realTopic, String realQid, String delay) {
        SyncRecord record = new SyncRecord();
        record.setTopic("SCHEDULE_TOPIC_XXXX");
        record.setPhysicOffset(1000);
        record.setQueueId(0);
        Map<String, String> props = new HashMap<>();
        props.put("REAL_TOPIC", realTopic);
        props.put("REAL_QID", realQid);
        if (delay != null) {
            props.put("DELAY", delay);
        }
        record.setProperties(props);
        return record;
    }
}
