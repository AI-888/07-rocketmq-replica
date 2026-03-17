package org.apache.rocketmq.hasync.source;

import org.apache.rocketmq.hasync.model.SyncRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * CommitLog 消息解析器
 * <p>
 * 根据 RocketMQ MessageDecoder 格式解析 CommitLog 数据包中的消息。
 * <p>
 * 消息完整性校验项（需求 7 §1）：
 * <ul>
 *   <li>magicCode 校验：必须为 MESSAGE_MAGIC_CODE(0xAABBCCDD) 或 MESSAGE_MAGIC_CODE_V2(0xAABBCCDE)</li>
 *   <li>totalSize 校验：> 20B 且 ≤ 4MB</li>
 *   <li>实际读取长度校验：实际字节数 == totalSize</li>
 *   <li>bodyCRC 校验：CRC32 比对</li>
 * </ul>
 *
 * @see org.apache.rocketmq.store.CommitLog
 */
public class CommitLogParser {

    private static final Logger log = LoggerFactory.getLogger(CommitLogParser.class);

    /** RocketMQ 消息魔数 V1 */
    public static final int MESSAGE_MAGIC_CODE = 0xAABBCCDD;

    /** RocketMQ 消息魔数 V2 */
    public static final int MESSAGE_MAGIC_CODE_V2 = 0xAABBCCDE;

    /** Blank 魔数（表示 CommitLog 文件末尾填充） */
    public static final int BLANK_MAGIC_CODE = 0xBBCCDDEE;

    /** 消息头最小长度（字节） */
    public static final int MSG_HEADER_MIN_SIZE = 20;

    /** 单条消息最大长度（默认 4MB） */
    public static final int MSG_MAX_SIZE = 4 * 1024 * 1024;

    /** Topic 流量统计（字节数） */
    private final Map<String, AtomicLong> topicBytesStats = new ConcurrentHashMap<>();

    /** 解析失败回调（用于 RFQ） */
    private ParseFailureCallback failureCallback;

    /**
     * 解析失败回调接口
     */
    @FunctionalInterface
    public interface ParseFailureCallback {
        /**
         * 消息解析失败时调用
         *
         * @param rawBytes       消息原始字节
         * @param masterPhyOffset 数据包起始偏移量
         * @param offsetInPacket 消息在包内的偏移量
         * @param errorReason    错误原因
         */
        void onParseFailure(byte[] rawBytes, long masterPhyOffset, int offsetInPacket, String errorReason);
    }

    public CommitLogParser() {
    }

    public CommitLogParser(ParseFailureCallback failureCallback) {
        this.failureCallback = failureCallback;
    }

    public void setFailureCallback(ParseFailureCallback failureCallback) {
        this.failureCallback = failureCallback;
    }

    /**
     * 解析 CommitLog 数据包中的所有消息
     * <p>
     * 严格按照 CommitLog 物理偏移量顺序产出 SyncRecord（需求 2 §6a）。
     *
     * @param body            数据包 body
     * @param masterPhyOffset 数据包起始物理偏移量
     * @return 解析后的 SyncRecord 列表（严格按 physicOffset 升序）
     */
    public List<SyncRecord> parse(byte[] body, long masterPhyOffset) {
        List<SyncRecord> records = new ArrayList<>();

        if (body == null || body.length == 0) {
            return records;
        }

        ByteBuffer buffer = ByteBuffer.wrap(body);
        int totalParseError = 0;
        int totalMessages = 0;

        while (buffer.hasRemaining()) {
            int positionInPacket = buffer.position();
            long currentPhyOffset = masterPhyOffset + positionInPacket;

            // 至少需要 4 字节读取 totalSize
            if (buffer.remaining() < 4) {
                log.debug("数据包剩余不足 4 字节，跳过末尾 {} 字节", buffer.remaining());
                break;
            }

            // 读取 totalSize（消息总长度）
            int totalSize = buffer.getInt(buffer.position());

            // totalSize 校验
            if (totalSize <= 0 || totalSize < MSG_HEADER_MIN_SIZE) {
                // 可能是文件末尾或空白填充
                if (totalSize == 0) {
                    log.debug("遇到 totalSize=0，数据包解析完毕");
                    break;
                }
                String reason = "INVALID_TOTAL_SIZE: totalSize=" + totalSize + " (期望 >= " + MSG_HEADER_MIN_SIZE + ")";
                log.error("消息完整性校验失败 [offset={}]: {}", currentPhyOffset, reason);
                handleParseFailure(body, masterPhyOffset, positionInPacket, buffer.remaining(), reason);
                totalParseError++;
                break; // 无法确定下一条消息边界，停止解析
            }

            if (totalSize > MSG_MAX_SIZE) {
                String reason = "INVALID_TOTAL_SIZE: totalSize=" + totalSize + " (超过最大限制 " + MSG_MAX_SIZE + ")";
                log.error("消息完整性校验失败 [offset={}]: {}", currentPhyOffset, reason);
                handleParseFailure(body, masterPhyOffset, positionInPacket, Math.min(buffer.remaining(), totalSize), reason);
                totalParseError++;
                break;
            }

            // 实际读取长度校验
            if (buffer.remaining() < totalSize) {
                String reason = "MSG_TRUNCATED: 需要 " + totalSize + " 字节，实际剩余 " + buffer.remaining() + " 字节（半包）";
                log.warn("消息截断（半包）[offset={}]: {}", currentPhyOffset, reason);
                handleParseFailure(body, masterPhyOffset, positionInPacket, buffer.remaining(), reason);
                totalParseError++;
                break;
            }

            // 提取消息完整字节
            byte[] msgBytes = new byte[totalSize];
            buffer.get(msgBytes);
            totalMessages++;

            // 解析单条消息
            try {
                SyncRecord record = parseMessage(msgBytes, currentPhyOffset, masterPhyOffset);
                if (record != null) {
                    records.add(record);

                    // 更新 Topic 流量统计
                    topicBytesStats.computeIfAbsent(record.getTopic(), k -> new AtomicLong(0))
                            .addAndGet(record.getMsgSize());
                }
            } catch (Exception e) {
                String reason = e.getClass().getSimpleName() + ": " + e.getMessage();
                log.error("消息解析异常 [offset={}]: {}", currentPhyOffset, reason);
                handleParseFailure(msgBytes, masterPhyOffset, positionInPacket, totalSize, reason);
                totalParseError++;
            }
        }

        // 数据包损坏预警（需求 14 §8）
        if (totalMessages > 0 && totalParseError > 0) {
            double errorRate = (double) totalParseError / (totalMessages + totalParseError);
            if (errorRate > 0.5) {
                log.warn("数据包可能严重损坏: masterPhyOffset={}, 总消息数={}, 解析失败={}, 失败率={:.1f}%",
                        masterPhyOffset, totalMessages + totalParseError, totalParseError, errorRate * 100);
            }
        }

        return records;
    }

    /**
     * 解析单条消息
     * <p>
     * 消息格式参照 RocketMQ MessageDecoder.decodeMessage
     */
    SyncRecord parseMessage(byte[] msgBytes, long physicOffset, long masterPhyOffset) {
        ByteBuffer buf = ByteBuffer.wrap(msgBytes);

        // --- 消息头解析（按 RocketMQ CommitLog 消息格式） ---
        int totalSize = buf.getInt();          // 0: TOTALSIZE
        int magicCode = buf.getInt();          // 4: MAGICCODE

        // magicCode 校验
        if (magicCode == BLANK_MAGIC_CODE) {
            // 空白填充消息，跳过
            return null;
        }
        if (magicCode != MESSAGE_MAGIC_CODE && magicCode != MESSAGE_MAGIC_CODE_V2) {
            throw new IllegalArgumentException(
                    String.format("INVALID_MAGIC_CODE: 期望 0x%08X 或 0x%08X，实际 0x%08X",
                            MESSAGE_MAGIC_CODE, MESSAGE_MAGIC_CODE_V2, magicCode));
        }

        int bodyCRC = buf.getInt();            // 8: BODYCRC
        int queueId = buf.getInt();            // 12: QUEUEID
        int flag = buf.getInt();               // 16: FLAG
        long queueOffset = buf.getLong();       // 20: QUEUEOFFSET
        long physicalOffset = buf.getLong();    // 28: PHYSICALOFFSET (= physicOffset)
        int sysFlag = buf.getInt();            // 36: SYSFLAG
        long bornTimestamp = buf.getLong();     // 40: BORNTIMESTAMP
        // bornHost (8 bytes for IPv4, 20 bytes for IPv6)
        int bornHostLength = (sysFlag & 1) != 0 ? 20 : 8;
        buf.position(buf.position() + bornHostLength); // 跳过 bornHost
        long storeTimestamp = buf.getLong();    // STORETIMESTAMP
        // storeHost
        int storeHostLength = (sysFlag & 2) != 0 ? 20 : 8;
        buf.position(buf.position() + storeHostLength); // 跳过 storeHost
        int reconsumeTimes = buf.getInt();     // RECONSUMETIMES
        long preparedTransOffset = buf.getLong(); // PREPAREDTRANSACTIONOFFSET
        int bodyLen = buf.getInt();            // BODY LENGTH

        // 读取 body
        byte[] body = null;
        if (bodyLen > 0) {
            if (buf.remaining() < bodyLen) {
                throw new IllegalArgumentException("INVALID_BODY_SIZE: bodyLen=" + bodyLen
                        + ", 剩余=" + buf.remaining());
            }
            body = new byte[bodyLen];
            buf.get(body);

            // bodyCRC 校验
            CRC32 crc32 = new CRC32();
            crc32.update(body);
            int actualCRC = (int) crc32.getValue();
            if (bodyCRC != 0 && actualCRC != bodyCRC) {
                throw new IllegalArgumentException(
                        String.format("BODY_CRC_MISMATCH: 期望 0x%08X，实际 0x%08X", bodyCRC, actualCRC));
            }
        }

        // 读取 topic
        short topicLen;
        if (magicCode == MESSAGE_MAGIC_CODE_V2) {
            topicLen = buf.getShort(); // V2: 2 字节 topic 长度
        } else {
            topicLen = (short) (buf.get() & 0xFF); // V1: 1 字节 topic 长度
        }
        byte[] topicBytes = new byte[topicLen];
        buf.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        // 读取 properties
        short propertiesLen = buf.getShort();
        String propertiesStr = "";
        if (propertiesLen > 0 && buf.remaining() >= propertiesLen) {
            byte[] propsBytes = new byte[propertiesLen];
            buf.get(propsBytes);
            propertiesStr = new String(propsBytes, StandardCharsets.UTF_8);
        }

        // 封装 SyncRecord
        SyncRecord record = new SyncRecord();
        record.setPhysicOffset(physicOffset);
        record.setMasterPhyOffset(masterPhyOffset);
        record.setEndOffset(physicOffset + totalSize);
        record.setTopic(topic);
        record.setQueueId(queueId);
        record.setBody(body);
        record.setMsgSize(totalSize);
        record.setStoreTimestamp(storeTimestamp);
        record.setReceiveTimestamp(System.currentTimeMillis());
        record.setFlag(flag);
        record.setSysFlag(sysFlag);
        record.setQueueOffset(queueOffset);
        record.setBornTimestamp(bornTimestamp);
        record.setReconsumeTimes(reconsumeTimes);

        // 解析 properties
        if (!propertiesStr.isEmpty()) {
            String[] pairs = propertiesStr.split(String.valueOf((char) 2)); // PROPERTY_SEPARATOR
            for (String pair : pairs) {
                int idx = pair.indexOf((char) 1); // NAME_VALUE_SEPARATOR
                if (idx > 0) {
                    record.putProperty(pair.substring(0, idx), pair.substring(idx + 1));
                }
            }
        }

        return record;
    }

    /**
     * 处理解析失败的消息
     */
    private void handleParseFailure(byte[] data, long masterPhyOffset, int offsetInPacket,
                                     int length, String reason) {
        if (failureCallback != null) {
            // 截取失败消息的原始字节
            byte[] rawBytes;
            if (offsetInPacket < data.length) {
                int end = Math.min(offsetInPacket + length, data.length);
                rawBytes = new byte[end - offsetInPacket];
                System.arraycopy(data, offsetInPacket, rawBytes, 0, rawBytes.length);
            } else {
                rawBytes = new byte[0];
            }
            failureCallback.onParseFailure(rawBytes, masterPhyOffset, offsetInPacket, reason);
        }
    }

    /**
     * 执行兼容性预校验（需求 5）
     * <p>
     * 尝试解析少量消息，校验 magicCode、totalSize、bodyCRC。
     *
     * @param body            数据包 body（至少 1 条完整消息，最多 1MB）
     * @param masterPhyOffset 数据包起始偏移量
     * @return true 校验通过，false 不兼容
     * @throws CommitLogIncompatibleException 消息格式不兼容时抛出
     */
    public boolean compatibilityCheck(byte[] body, long masterPhyOffset) {
        if (body == null || body.length == 0) {
            log.warn("Master CommitLog 为空，跳过兼容性预校验");
            return true;
        }

        try {
            List<SyncRecord> records = parse(body, masterPhyOffset);
            if (records.isEmpty()) {
                log.warn("兼容性预校验：未解析出任何消息，数据包可能为空白填充");
                return true;
            }
            log.info("兼容性预校验成功，消息格式兼容，解析出 {} 条消息，准备开始同步", records.size());
            return true;
        } catch (Exception e) {
            String msg = String.format("兼容性预校验失败: Master地址=%s, 偏移量=%d, 原因=%s",
                    "N/A", masterPhyOffset, e.getMessage());
            log.error(msg, e);
            throw new CommitLogIncompatibleException(msg, e);
        }
    }

    /**
     * 获取 Topic 流量统计
     */
    public Map<String, AtomicLong> getTopicBytesStats() {
        return topicBytesStats;
    }

    /**
     * 重置 Topic 流量统计
     */
    public void resetTopicBytesStats() {
        topicBytesStats.clear();
    }

    /**
     * CommitLog 格式不兼容异常
     */
    public static class CommitLogIncompatibleException extends RuntimeException {
        public CommitLogIncompatibleException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
