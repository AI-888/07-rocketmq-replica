package org.apache.rocketmq.hasync.source;

import org.apache.rocketmq.hasync.model.ReplicaFailRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * RfqSink + ReplicaFailRecord + SourceRegistry 单元测试
 */
public class RfqSinkTest {

    // ==================== ReplicaFailRecord 测试 ====================

    @Test
    public void testReplicaFailRecordCreation() {
        byte[] raw = {0x01, 0x02, 0x03};
        ReplicaFailRecord record = new ReplicaFailRecord(
                raw, 1000L, 50, "INVALID_MAGIC_CODE", "127.0.0.1:9876");

        assertEquals(1000L, record.getMasterPhyOffset());
        assertEquals(50, record.getOffsetInPacket());
        assertEquals("INVALID_MAGIC_CODE", record.getErrorReason());
        assertEquals("127.0.0.1:9876", record.getSourceCluster());
        assertNotNull(record.getFailTimestamp());
        assertArrayEquals(raw, record.getRawBytes());
    }

    @Test
    public void testReplicaFailRecordSerialization() {
        ReplicaFailRecord record = new ReplicaFailRecord(
                new byte[]{0x0A, 0x0B}, 2000L, 100, "BODY_CRC_MISMATCH", "192.168.1.1:9876");

        byte[] json = record.toJsonBytes();
        assertNotNull(json);
        assertTrue(json.length > 0);

        ReplicaFailRecord deserialized = ReplicaFailRecord.fromJsonBytes(json);
        assertEquals(record.getMasterPhyOffset(), deserialized.getMasterPhyOffset());
        assertEquals(record.getOffsetInPacket(), deserialized.getOffsetInPacket());
        assertEquals(record.getErrorReason(), deserialized.getErrorReason());
        assertEquals(record.getSourceCluster(), deserialized.getSourceCluster());
    }

    @Test
    public void testReplicaFailRecordToString() {
        ReplicaFailRecord record = new ReplicaFailRecord(
                new byte[10], 3000L, 0, "MSG_TRUNCATED", "10.0.0.1:9876");
        String str = record.toString();
        assertTrue(str.contains("3000"));
        assertTrue(str.contains("MSG_TRUNCATED"));
        assertTrue(str.contains("10"));
    }

    // ==================== RfqSink 测试 ====================

    private RfqSink rfqSink;
    private MockRfqSendCallback sendCallback;

    @Before
    public void setUp() {
        sendCallback = new MockRfqSendCallback();
        rfqSink = new RfqSink("ha-sync-rfq", "rfq-producer", 3, "127.0.0.1:9876");
        rfqSink.setSendCallback(sendCallback);
        rfqSink.start();
    }

    @Test
    public void testRfqSinkSendSuccess() {
        ReplicaFailRecord record = new ReplicaFailRecord(
                new byte[]{1, 2}, 1000L, 0, "TEST_ERROR", "127.0.0.1:9876");

        rfqSink.sendToRfq(record);

        assertEquals(1, rfqSink.getRfqSendSuccessCount());
        assertEquals(0, rfqSink.getRfqSendFailureCount());
        assertEquals(1, sendCallback.sentRecords.size());
    }

    @Test
    public void testRfqSinkRetryThenSuccess() {
        sendCallback.failCount = 2; // 前 2 次失败

        ReplicaFailRecord record = new ReplicaFailRecord(
                new byte[]{1}, 2000L, 0, "RETRY_TEST", "127.0.0.1:9876");

        rfqSink.sendToRfq(record);

        assertEquals(1, rfqSink.getRfqSendSuccessCount());
        assertEquals(0, rfqSink.getRfqSendFailureCount());
    }

    @Test
    public void testRfqSinkAllRetryFail() {
        sendCallback.failAll = true;

        ReplicaFailRecord record = new ReplicaFailRecord(
                new byte[]{1}, 3000L, 0, "ALL_FAIL", "127.0.0.1:9876");

        rfqSink.sendToRfq(record);

        assertEquals(0, rfqSink.getRfqSendSuccessCount());
        assertEquals(1, rfqSink.getRfqSendFailureCount());
        assertTrue(rfqSink.getRfqFallbackCount() > 0);

        // 清理备用文件
        new File("./rfq-fallback.jsonl").delete();
    }

    @Test
    public void testRfqSinkNotStarted() {
        RfqSink notStarted = new RfqSink("topic", "group", 3, "addr");
        ReplicaFailRecord record = new ReplicaFailRecord(
                new byte[]{1}, 0L, 0, "ERR", "addr");

        notStarted.sendToRfq(record); // 不应抛异常

        // 清理备用文件
        new File("./rfq-fallback.jsonl").delete();
    }

    @Test
    public void testRfqSinkProperties() {
        assertEquals("ha-sync-rfq", rfqSink.getRfqTopic());
        assertEquals("rfq-producer", rfqSink.getRfqProducerGroup());
        assertTrue(rfqSink.isStarted());

        rfqSink.stop();
        assertFalse(rfqSink.isStarted());
    }

    @Test
    public void testRfqSinkMessageProperties() {
        ReplicaFailRecord record = new ReplicaFailRecord(
                new byte[]{1}, 5000L, 10, "CRC_ERROR", "10.0.0.1:9876");

        rfqSink.sendToRfq(record);

        Map<String, String> props = sendCallback.lastProperties;
        assertNotNull(props);
        assertEquals("5000", props.get("MASTER_PHY_OFFSET"));
        assertEquals("CRC_ERROR", props.get("ERROR_REASON"));
        assertEquals("10.0.0.1:9876", props.get("SOURCE_CLUSTER"));
        assertNotNull(props.get("FAIL_TIMESTAMP"));
    }

    @Test
    public void testAsParseFailureCallback() {
        CommitLogParser.ParseFailureCallback cb = rfqSink.asParseFailureCallback();
        assertNotNull(cb);

        cb.onParseFailure(new byte[]{0x01}, 1000L, 50, "TEST_REASON");

        assertEquals(1, rfqSink.getRfqSendSuccessCount());
    }

    // ==================== SourceRegistry 测试 ====================

    @Test
    public void testSourceRegistryRegister() throws Exception {
        MockRegistryCallback registryCb = new MockRegistryCallback();
        SourceRegistry registry = new SourceRegistry("127.0.0.1:9876", "broker-a", "127.0.0.1", 5555);
        registry.setCallback(registryCb);

        registry.register();

        assertTrue(registry.isRegistered());
        assertEquals("SYNC_SOURCE_CONFIG", registryCb.lastNamespace);
        assertEquals("broker-a", registryCb.lastKey);
        assertTrue(registryCb.lastValue.startsWith("127.0.0.1:5555:"));

        registry.unregister();
        assertFalse(registry.isRegistered());
    }

    @Test
    public void testSourceRegistryGetters() {
        SourceRegistry registry = new SourceRegistry("10.0.0.1:9876", "broker-b", "10.0.0.2", 6666);
        assertEquals("10.0.0.1:9876", registry.getTargetNamesrvAddr());
        assertEquals("broker-b", registry.getBrokerName());
        assertEquals("10.0.0.2:6666", registry.getZmqAddress());
        assertEquals("SYNC_SOURCE_CONFIG", SourceRegistry.getNamespace());
    }

    @Test(expected = IllegalStateException.class)
    public void testSourceRegistryNoCallback() throws Exception {
        SourceRegistry registry = new SourceRegistry("addr", "broker", "host", 5555);
        registry.register(); // 应抛 IllegalStateException
    }

    @Test
    public void testSourceRegistryUnregisterNotRegistered() {
        SourceRegistry registry = new SourceRegistry("addr", "broker", "host", 5555);
        registry.unregister(); // 不应抛异常
    }

    // ==================== Mock 实现 ====================

    static class MockRfqSendCallback implements RfqSink.RfqSendCallback {
        List<byte[]> sentRecords = new ArrayList<>();
        Map<String, String> lastProperties;
        boolean failAll = false;
        int failCount = 0;
        final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public void send(String topic, byte[] body, Map<String, String> properties) throws Exception {
            int count = callCount.incrementAndGet();
            if (failAll || count <= failCount) {
                throw new RuntimeException("模拟发送失败 #" + count);
            }
            sentRecords.add(body);
            lastProperties = properties;
        }
    }

    static class MockRegistryCallback implements SourceRegistry.RegistryCallback {
        String lastNamespace;
        String lastKey;
        String lastValue;
        boolean deleted = false;

        @Override
        public void putKVConfig(String namespace, String key, String value) {
            this.lastNamespace = namespace;
            this.lastKey = key;
            this.lastValue = value;
        }

        @Override
        public void deleteKVConfig(String namespace, String key) {
            this.deleted = true;
        }
    }
}
