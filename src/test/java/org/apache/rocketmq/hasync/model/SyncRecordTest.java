package org.apache.rocketmq.hasync.model;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * SyncRecord 模型单元测试
 */
public class SyncRecordTest {

    @Test
    public void testDefaultConstructor() {
        SyncRecord record = new SyncRecord();
        assertNotNull("properties 应在构造时初始化", record.getProperties());
        assertTrue("properties 初始应为空", record.getProperties().isEmpty());
        assertEquals(0L, record.getMasterPhyOffset());
        assertEquals(0L, record.getEndOffset());
        assertEquals(0L, record.getPhysicOffset());
        assertNull(record.getTopic());
        assertEquals(0, record.getQueueId());
        assertNull(record.getBody());
        assertEquals(0, record.getMsgSize());
        assertEquals(0L, record.getStoreTimestamp());
        assertEquals(0L, record.getReceiveTimestamp());
        assertNull(record.getTraceId());
    }

    @Test
    public void testSettersAndGetters() {
        SyncRecord record = new SyncRecord();

        record.setMasterPhyOffset(1000L);
        assertEquals(1000L, record.getMasterPhyOffset());

        record.setEndOffset(2000L);
        assertEquals(2000L, record.getEndOffset());

        record.setPhysicOffset(1500L);
        assertEquals(1500L, record.getPhysicOffset());

        record.setTopic("test-topic");
        assertEquals("test-topic", record.getTopic());

        record.setQueueId(3);
        assertEquals(3, record.getQueueId());

        byte[] body = {1, 2, 3, 4, 5};
        record.setBody(body);
        assertArrayEquals(body, record.getBody());

        record.setMsgSize(1024);
        assertEquals(1024, record.getMsgSize());

        record.setStoreTimestamp(System.currentTimeMillis());
        assertTrue(record.getStoreTimestamp() > 0);

        record.setReceiveTimestamp(System.currentTimeMillis());
        assertTrue(record.getReceiveTimestamp() > 0);

        record.setTraceId("node1-1000-0");
        assertEquals("node1-1000-0", record.getTraceId());
    }

    @Test
    public void testPutProperty() {
        SyncRecord record = new SyncRecord();
        record.putProperty("key1", "value1");
        record.putProperty("key2", "value2");

        assertEquals("value1", record.getProperties().get("key1"));
        assertEquals("value2", record.getProperties().get("key2"));
        assertEquals(2, record.getProperties().size());
    }

    @Test
    public void testPutPropertyWhenPropertiesNull() {
        SyncRecord record = new SyncRecord();
        record.setProperties(null);
        // 调用 putProperty 应自动初始化
        record.putProperty("key", "value");
        assertNotNull(record.getProperties());
        assertEquals("value", record.getProperties().get("key"));
    }

    @Test
    public void testSetProperties() {
        SyncRecord record = new SyncRecord();
        Map<String, String> props = new HashMap<>();
        props.put("a", "1");
        props.put("b", "2");
        record.setProperties(props);

        assertEquals(props, record.getProperties());
    }

    @Test
    public void testToString() {
        SyncRecord record = new SyncRecord();
        record.setMasterPhyOffset(100L);
        record.setEndOffset(200L);
        record.setPhysicOffset(150L);
        record.setTopic("my-topic");
        record.setQueueId(2);
        record.setMsgSize(512);
        record.setTraceId("trace-001");

        String str = record.toString();
        assertTrue(str.contains("masterPhyOffset=100"));
        assertTrue(str.contains("endOffset=200"));
        assertTrue(str.contains("physicOffset=150"));
        assertTrue(str.contains("my-topic"));
        assertTrue(str.contains("queueId=2"));
        assertTrue(str.contains("msgSize=512"));
        assertTrue(str.contains("trace-001"));
    }
}
