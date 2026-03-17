package org.apache.rocketmq.hasync.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HA 协议 TCP 连接管理器
 * <p>
 * 模拟 RocketMQ Slave 的 HA 连接行为：
 * <ul>
 *   <li>仅使用 DefaultHAService 基础 Slave 协议</li>
 *   <li>不发送 HANDSHAKE 包，不携带 slaveAddress</li>
 *   <li>不向 NameServer 注册为 Broker</li>
 *   <li>不被纳入 SyncStateSet</li>
 * </ul>
 * <p>
 * 数据包格式（Master → Slave）：
 * <pre>
 *   masterPhyOffset (8 bytes) + bodySize (4 bytes) + body (variable)
 * </pre>
 * <p>
 * 上报格式（Slave → Master）：
 * <pre>
 *   slaveMaxOffset (8 bytes)
 * </pre>
 *
 * @see <a href="https://github.com/apache/rocketmq">DefaultHAService</a>
 */
public class HASourceConnection {

    private static final Logger log = LoggerFactory.getLogger(HASourceConnection.class);

    /** 数据包头部大小：masterPhyOffset(8) + bodySize(4) = 12 字节 */
    public static final int PACKET_HEADER_SIZE = 12;

    /** 接收缓冲区默认大小（4MB） */
    private static final int RECV_BUFFER_SIZE = 4 * 1024 * 1024;

    /** slaveMaxOffset 上报消息大小（8 字节） */
    private static final int REPORT_OFFSET_SIZE = 8;

    private SocketChannel socketChannel;
    private final ByteBuffer recvBuffer;
    private final ByteBuffer reportBuffer;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicLong lastRecvTimestamp = new AtomicLong(0);
    private String currentMasterAddr;

    public HASourceConnection() {
        this.recvBuffer = ByteBuffer.allocate(RECV_BUFFER_SIZE);
        this.reportBuffer = ByteBuffer.allocate(REPORT_OFFSET_SIZE);
    }

    /**
     * 连接到 Master HA 服务地址
     * <p>
     * 仅使用 DefaultHAService 基础 Slave 协议，不发送 HANDSHAKE 包。
     *
     * @param masterHaAddr Master HA 地址（格式：host:port）
     * @throws IOException 连接失败
     */
    public void connect(String masterHaAddr) throws IOException {
        if (connected.get()) {
            close();
        }

        String[] parts = masterHaAddr.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("无效的 Master HA 地址格式: " + masterHaAddr + "（期望 host:port）");
        }

        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        log.info("连接 Master HA 服务: {} (仅使用 DefaultHAService 基础 Slave 协议，不参与选举)", masterHaAddr);

        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(true);
        socketChannel.socket().setSoTimeout(30000);
        socketChannel.socket().setReceiveBufferSize(RECV_BUFFER_SIZE);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setKeepAlive(true);

        socketChannel.connect(new InetSocketAddress(host, port));

        connected.set(true);
        currentMasterAddr = masterHaAddr;
        lastRecvTimestamp.set(System.currentTimeMillis());

        log.info("已连接 Master HA 服务: {} (不向 NameServer 注册为 Broker，不发送 Broker 心跳)", masterHaAddr);
    }

    /**
     * 向 Master 上报 slaveMaxOffset
     * <p>
     * 上报的是 confirmedOffset（已确认写入目标集群的位点），
     * 而非内存中尚未确认的接收位点。
     *
     * @param slaveMaxOffset 已确认位点
     * @throws IOException 网络异常
     */
    public void reportSlaveMaxOffset(long slaveMaxOffset) throws IOException {
        if (!connected.get()) {
            throw new IOException("未连接到 Master");
        }

        reportBuffer.clear();
        reportBuffer.putLong(slaveMaxOffset);
        reportBuffer.flip();

        while (reportBuffer.hasRemaining()) {
            socketChannel.write(reportBuffer);
        }
    }

    /**
     * 接收 Master 数据包
     * <p>
     * 数据包格式：masterPhyOffset(8) + bodySize(4) + body(variable)
     *
     * @return 数据包，null 表示没有数据可读
     * @throws IOException 网络异常或半包
     */
    public HADataPacket receive() throws IOException {
        if (!connected.get()) {
            throw new IOException("未连接到 Master");
        }

        // 读取包头（12 字节）
        ByteBuffer headerBuf = ByteBuffer.allocate(PACKET_HEADER_SIZE);
        readFully(headerBuf);
        headerBuf.flip();

        long masterPhyOffset = headerBuf.getLong();
        int bodySize = headerBuf.getInt();

        if (bodySize <= 0) {
            // 心跳包（空数据包）
            lastRecvTimestamp.set(System.currentTimeMillis());
            return null;
        }

        if (bodySize > RECV_BUFFER_SIZE) {
            throw new IOException("数据包 bodySize 过大: " + bodySize + " > " + RECV_BUFFER_SIZE);
        }

        // 读取 body
        ByteBuffer bodyBuf = ByteBuffer.allocate(bodySize);
        readFully(bodyBuf);
        bodyBuf.flip();

        byte[] body = new byte[bodySize];
        bodyBuf.get(body);

        lastRecvTimestamp.set(System.currentTimeMillis());

        return new HADataPacket(masterPhyOffset, body);
    }

    /**
     * 从 SocketChannel 完整读取指定长度的数据
     */
    private void readFully(ByteBuffer buffer) throws IOException {
        int maxRetry = 100;
        int retryCount = 0;

        while (buffer.hasRemaining()) {
            int read = socketChannel.read(buffer);
            if (read == -1) {
                throw new IOException("Master 连接已关闭（EOF）");
            }
            if (read == 0) {
                retryCount++;
                if (retryCount > maxRetry) {
                    throw new IOException("读取超时：已重试 " + maxRetry + " 次仍无数据");
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("读取被中断", e);
                }
            } else {
                retryCount = 0;
            }
        }
    }

    /**
     * 关闭连接
     */
    public void close() {
        connected.set(false);
        if (socketChannel != null) {
            try {
                socketChannel.close();
            } catch (IOException e) {
                log.warn("关闭 SocketChannel 时出错: {}", e.getMessage());
            }
        }
        log.info("已断开与 Master {} 的连接", currentMasterAddr);
    }

    // ==================== Getters ====================

    public boolean isConnected() {
        return connected.get();
    }

    public String getCurrentMasterAddr() {
        return currentMasterAddr;
    }

    public long getLastRecvTimestamp() {
        return lastRecvTimestamp.get();
    }

    /**
     * HA 数据包
     */
    public static class HADataPacket {
        private final long masterPhyOffset;
        private final byte[] body;

        public HADataPacket(long masterPhyOffset, byte[] body) {
            this.masterPhyOffset = masterPhyOffset;
            this.body = body;
        }

        public long getMasterPhyOffset() {
            return masterPhyOffset;
        }

        public byte[] getBody() {
            return body;
        }

        public int getBodySize() {
            return body != null ? body.length : 0;
        }
    }
}
