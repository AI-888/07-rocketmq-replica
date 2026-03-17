package org.apache.rocketmq.hasync.source;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * HASourceConnection 单元测试
 */
public class HASourceConnectionTest {

    private HASourceConnection connection;

    @Before
    public void setUp() {
        connection = new HASourceConnection();
    }

    @Test
    public void testInitialState() {
        assertFalse(connection.isConnected());
        assertNull(connection.getCurrentMasterAddr());
    }

    @Test(expected = Exception.class)
    public void testConnectToInvalidAddr() throws Exception {
        connection.connect("invalid-host:99999");
    }

    @Test
    public void testCloseNotConnected() {
        // close() 在未连接时不应抛异常
        connection.close();
        assertFalse(connection.isConnected());
    }

    @Test(expected = IOException.class)
    public void testReportSlaveMaxOffsetNotConnected() throws Exception {
        connection.reportSlaveMaxOffset(12345L);
    }

    @Test(expected = IOException.class)
    public void testReceiveNotConnected() throws Exception {
        connection.receive();
    }

    @Test
    public void testHADataPacket() {
        byte[] body = {0x01, 0x02, 0x03};
        HASourceConnection.HADataPacket packet =
                new HASourceConnection.HADataPacket(1000L, body);

        assertEquals(1000L, packet.getMasterPhyOffset());
        assertArrayEquals(body, packet.getBody());
        assertEquals(3, packet.getBodySize());
    }

    @Test
    public void testHADataPacketNullBody() {
        HASourceConnection.HADataPacket packet =
                new HASourceConnection.HADataPacket(0L, null);

        assertNull(packet.getBody());
        assertEquals(0, packet.getBodySize());
    }

    @Test
    public void testPacketHeaderSize() {
        assertEquals(12, HASourceConnection.PACKET_HEADER_SIZE);
    }
}
