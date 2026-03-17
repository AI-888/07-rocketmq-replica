package org.apache.rocketmq.hasync.source;

import org.apache.rocketmq.hasync.model.SyncRecord;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import static org.junit.Assert.*;

/**
 * CommitLogParser 单元测试
 * <p>
 * 覆盖需求 7（消息完整性与顺序）的全部校验项。
 */
public class CommitLogParserTest {

    private CommitLogParser parser;
    private List<String> parseFailures;

    @Before
    public void setUp() {
        parseFailures = new ArrayList<>();
        parser = new CommitLogParser((rawBytes, masterPhyOffset, offsetInPacket, errorReason) -> {
            parseFailures.add(errorReason);
        });
    }

    // ==================== 正常解析 ====================

    @Test
    public void testParseValidV1Message() {
        byte[] packet = buildValidMessage("test-topic", "hello", 0xAABBCCDD, 0);
        List<SyncRecord> records = parser.parse(packet, 1000L);

        assertEquals(1, records.size());
        SyncRecord r = records.get(0);
        assertEquals("test-topic", r.getTopic());
        assertEquals(1000L, r.getPhysicOffset());
        assertNotNull(r.getBody());
        assertEquals("hello", new String(r.getBody()));
        assertEquals(0, r.getQueueId());
    }

    @Test
    public void testParseValidV2Message() {
        byte[] packet = buildValidMessage("topic-v2", "body-v2", 0xAABBCCDE, 0);
        List<SyncRecord> records = parser.parse(packet, 2000L);

        assertEquals(1, records.size());
        assertEquals("topic-v2", records.get(0).getTopic());
    }

    @Test
    public void testParseMultipleMessages() {
        byte[] msg1 = buildValidMessage("topic-a", "body-1", 0xAABBCCDD, 0);
        byte[] msg2 = buildValidMessage("topic-b", "body-2", 0xAABBCCDD, 1);

        byte[] combined = new byte[msg1.length + msg2.length];
        System.arraycopy(msg1, 0, combined, 0, msg1.length);
        System.arraycopy(msg2, 0, combined, msg1.length, msg2.length);

        List<SyncRecord> records = parser.parse(combined, 0L);

        assertEquals(2, records.size());
        assertEquals("topic-a", records.get(0).getTopic());
        assertEquals("topic-b", records.get(1).getTopic());

        // 验证顺序（需求 2 §6a：严格按 physicOffset 升序）
        assertTrue(records.get(0).getPhysicOffset() < records.get(1).getPhysicOffset());
    }

    @Test
    public void testParseEmptyBody() {
        List<SyncRecord> records = parser.parse(new byte[0], 0L);
        assertTrue(records.isEmpty());
    }

    @Test
    public void testParseNull() {
        List<SyncRecord> records = parser.parse(null, 0L);
        assertTrue(records.isEmpty());
    }

    // ==================== magicCode 校验 ====================

    @Test
    public void testInvalidMagicCode() {
        byte[] packet = buildValidMessage("topic", "body", 0x12345678, 0);
        List<SyncRecord> records = parser.parse(packet, 0L);

        // magicCode 无效 → 解析失败，但不应崩溃
        assertTrue(records.isEmpty() || parseFailures.size() > 0);
    }

    @Test
    public void testBlankMagicCode() {
        byte[] packet = buildValidMessage("topic", "body", 0xBBCCDDEE, 0);
        List<SyncRecord> records = parser.parse(packet, 0L);

        // BLANK_MAGIC_CODE → 空白填充，应跳过
        assertTrue(records.isEmpty());
    }

    // ==================== totalSize 校验 ====================

    @Test
    public void testTotalSizeZero() {
        byte[] packet = new byte[4];
        ByteBuffer.wrap(packet).putInt(0);

        List<SyncRecord> records = parser.parse(packet, 0L);
        assertTrue(records.isEmpty());
    }

    @Test
    public void testTotalSizeTooSmall() {
        byte[] packet = new byte[4];
        ByteBuffer.wrap(packet).putInt(10); // < MSG_HEADER_MIN_SIZE

        List<SyncRecord> records = parser.parse(packet, 0L);
        assertTrue(records.isEmpty());
        assertTrue(parseFailures.size() > 0);
        assertTrue(parseFailures.get(0).contains("INVALID_TOTAL_SIZE"));
    }

    @Test
    public void testTotalSizeTooLarge() {
        byte[] packet = new byte[4];
        ByteBuffer.wrap(packet).putInt(5 * 1024 * 1024); // > 4MB

        List<SyncRecord> records = parser.parse(packet, 0L);
        assertTrue(records.isEmpty());
        assertTrue(parseFailures.size() > 0);
    }

    // ==================== 消息截断（半包） ====================

    @Test
    public void testMessageTruncated() {
        byte[] fullMsg = buildValidMessage("topic", "body", 0xAABBCCDD, 0);
        // 截断为一半
        byte[] truncated = new byte[fullMsg.length / 2];
        System.arraycopy(fullMsg, 0, truncated, 0, truncated.length);

        List<SyncRecord> records = parser.parse(truncated, 0L);
        assertTrue(records.isEmpty());
        assertTrue(parseFailures.size() > 0);
    }

    // ==================== bodyCRC 校验 ====================

    @Test
    public void testBodyCRCMismatch() {
        byte[] packet = buildMessageWithBadCRC("topic", "body");

        List<SyncRecord> records = parser.parse(packet, 0L);
        assertTrue(records.isEmpty() || parseFailures.size() > 0);
    }

    // ==================== 兼容性预校验 ====================

    @Test
    public void testCompatibilityCheckSuccess() {
        byte[] packet = buildValidMessage("test", "hello", 0xAABBCCDD, 0);
        assertTrue(parser.compatibilityCheck(packet, 0L));
    }

    @Test
    public void testCompatibilityCheckEmpty() {
        assertTrue(parser.compatibilityCheck(new byte[0], 0L));
    }

    @Test
    public void testCompatibilityCheckNull() {
        assertTrue(parser.compatibilityCheck(null, 0L));
    }

    // ==================== Topic 流量统计 ====================

    @Test
    public void testTopicBytesStats() {
        byte[] msg1 = buildValidMessage("topic-stats", "hello-world-123", 0xAABBCCDD, 0);
        parser.parse(msg1, 0L);

        Map<String, AtomicLong> stats = parser.getTopicBytesStats();
        assertTrue(stats.containsKey("topic-stats"));
        assertTrue(stats.get("topic-stats").get() > 0);
    }

    @Test
    public void testResetTopicBytesStats() {
        byte[] msg = buildValidMessage("topic", "body", 0xAABBCCDD, 0);
        parser.parse(msg, 0L);

        assertFalse(parser.getTopicBytesStats().isEmpty());
        parser.resetTopicBytesStats();
        assertTrue(parser.getTopicBytesStats().isEmpty());
    }

    // ==================== SyncRecord 字段验证 ====================

    @Test
    public void testSyncRecordFields() {
        byte[] packet = buildValidMessage("field-test", "test-body", 0xAABBCCDD, 3);
        List<SyncRecord> records = parser.parse(packet, 5000L);

        assertEquals(1, records.size());
        SyncRecord r = records.get(0);

        assertEquals(5000L, r.getPhysicOffset());
        assertEquals(5000L, r.getMasterPhyOffset());
        assertEquals("field-test", r.getTopic());
        assertEquals(3, r.getQueueId());
        assertNotNull(r.getBody());
        assertTrue(r.getMsgSize() > 0);
        assertTrue(r.getReceiveTimestamp() > 0);
        assertTrue(r.getEndOffset() > r.getPhysicOffset());
    }

    // ==================== 构建测试消息 ====================

    /**
     * 构建一条有效的 RocketMQ CommitLog 格式消息
     */
    private byte[] buildValidMessage(String topic, String bodyStr, int magicCode, int queueId) {
        byte[] body = bodyStr.getBytes();
        byte[] topicBytes = topic.getBytes();

        CRC32 crc32 = new CRC32();
        crc32.update(body);
        int bodyCRC = (int) crc32.getValue();

        boolean isV2 = (magicCode == 0xAABBCCDE);

        // 计算 totalSize
        int bornHostLen = 8;  // IPv4
        int storeHostLen = 8; // IPv4
        int topicLenSize = isV2 ? 2 : 1;
        int propertiesLen = 0;

        int totalSize = 4   // TOTALSIZE
                + 4   // MAGICCODE
                + 4   // BODYCRC
                + 4   // QUEUEID
                + 4   // FLAG
                + 8   // QUEUEOFFSET
                + 8   // PHYSICALOFFSET
                + 4   // SYSFLAG
                + 8   // BORNTIMESTAMP
                + bornHostLen  // BORNHOST
                + 8   // STORETIMESTAMP
                + storeHostLen // STOREHOST
                + 4   // RECONSUMETIMES
                + 8   // PREPAREDTRANSACTIONOFFSET
                + 4   // BODYLENGTH
                + body.length
                + topicLenSize
                + topicBytes.length
                + 2   // PROPERTIESLENGTH
                + propertiesLen;

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putInt(totalSize);           // TOTALSIZE
        buf.putInt(magicCode);           // MAGICCODE
        buf.putInt(bodyCRC);             // BODYCRC
        buf.putInt(queueId);             // QUEUEID
        buf.putInt(0);                   // FLAG
        buf.putLong(0L);                 // QUEUEOFFSET
        buf.putLong(0L);                 // PHYSICALOFFSET
        buf.putInt(0);                   // SYSFLAG (IPv4)
        buf.putLong(System.currentTimeMillis()); // BORNTIMESTAMP
        buf.put(new byte[bornHostLen]);  // BORNHOST
        buf.putLong(System.currentTimeMillis()); // STORETIMESTAMP
        buf.put(new byte[storeHostLen]); // STOREHOST
        buf.putInt(0);                   // RECONSUMETIMES
        buf.putLong(0L);                 // PREPAREDTRANSACTIONOFFSET
        buf.putInt(body.length);         // BODYLENGTH
        buf.put(body);                   // BODY
        if (isV2) {
            buf.putShort((short) topicBytes.length);  // TOPIC LENGTH (V2: 2 bytes)
        } else {
            buf.put((byte) topicBytes.length);         // TOPIC LENGTH (V1: 1 byte)
        }
        buf.put(topicBytes);             // TOPIC
        buf.putShort((short) propertiesLen); // PROPERTIESLENGTH

        return buf.array();
    }

    /**
     * 构建一条 bodyCRC 不匹配的消息
     */
    private byte[] buildMessageWithBadCRC(String topic, String bodyStr) {
        byte[] packet = buildValidMessage(topic, bodyStr, 0xAABBCCDD, 0);
        // 篡改 bodyCRC（偏移 8）
        ByteBuffer buf = ByteBuffer.wrap(packet);
        buf.putInt(8, 0x12345678);
        return packet;
    }
}
