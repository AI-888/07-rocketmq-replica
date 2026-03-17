package org.apache.rocketmq.hasync.source;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * MasterDiscovery 单元测试
 * <p>
 * 覆盖需求 4（动态发现 Master 地址）和需求 8（Master 切换重连）。
 */
public class MasterDiscoveryTest {

    private MasterDiscovery discovery;
    private MockDiscoveryCallback callback;

    @Before
    public void setUp() {
        callback = new MockDiscoveryCallback();
        discovery = new MasterDiscovery("127.0.0.1:9876", "broker-a", callback);
    }

    @Test
    public void testDiscoverMasterHaAddr() throws Exception {
        callback.setMasterAddr("192.168.1.100:10912");
        callback.setHaAddr("192.168.1.100:10912");

        String haAddr = discovery.discoverMasterHaAddr(3);
        assertEquals("192.168.1.100:10912", haAddr);
        assertEquals("192.168.1.100:10912", discovery.getCurrentMasterHaAddr());
    }

    @Test
    public void testDiscoverWithBrokerName() throws Exception {
        callback.setMasterAddr("10.0.0.1:10911");
        callback.setHaAddr("10.0.0.1:10912");

        String haAddr = discovery.discoverMasterHaAddr(3);
        assertEquals("10.0.0.1:10912", haAddr);
    }

    @Test(expected = RuntimeException.class)
    public void testDiscoverFailsAllRetries() throws Exception {
        callback.setFailAll(true);
        discovery.discoverMasterHaAddr(2);
    }

    @Test
    public void testDiscoverRetryThenSuccess() throws Exception {
        callback.setFailCount(2); // 前 2 次失败，第 3 次成功
        callback.setMasterAddr("10.0.0.1:10911");
        callback.setHaAddr("10.0.0.1:10912");

        String haAddr = discovery.discoverMasterHaAddr(5);
        assertEquals("10.0.0.1:10912", haAddr);
        assertEquals(2, discovery.getNameSrvQueryErrorCount());
    }

    @Test
    public void testMasterSwitchDetection() throws Exception {
        callback.setMasterAddr("10.0.0.1:10911");
        callback.setHaAddr("10.0.0.1:10912");
        discovery.discoverMasterHaAddr(1);

        // 切换 Master
        callback.setMasterAddr("10.0.0.2:10911");
        callback.setHaAddr("10.0.0.2:10912");

        boolean changed = discovery.checkMasterChanged();
        assertTrue(changed);
        assertEquals("10.0.0.2:10912", discovery.getCurrentMasterHaAddr());
        assertEquals(1, discovery.getMasterSwitchCount());
    }

    @Test
    public void testMasterNoChange() throws Exception {
        callback.setMasterAddr("10.0.0.1:10911");
        callback.setHaAddr("10.0.0.1:10912");
        discovery.discoverMasterHaAddr(1);

        boolean changed = discovery.checkMasterChanged();
        assertFalse(changed);
        assertEquals(0, discovery.getMasterSwitchCount());
    }

    @Test
    public void testCheckMasterChangedFailure() throws Exception {
        callback.setMasterAddr("10.0.0.1:10911");
        callback.setHaAddr("10.0.0.1:10912");
        discovery.discoverMasterHaAddr(1);

        // 模拟查询失败
        callback.setFailAll(true);
        boolean changed = discovery.checkMasterChanged();
        assertFalse(changed); // 查询失败不应认为变更
        assertTrue(discovery.getNameSrvQueryErrorCount() > 0);
    }

    @Test
    public void testGetters() {
        assertEquals("127.0.0.1:9876", discovery.getNamesrvAddr());
        assertEquals("broker-a", discovery.getBrokerName());
    }

    @Test(expected = RuntimeException.class)
    public void testNoCallbackThrows() throws Exception {
        MasterDiscovery noCallback = new MasterDiscovery("addr", "broker");
        noCallback.discoverMasterHaAddr(1);
    }

    // ==================== Mock 实现 ====================

    static class MockDiscoveryCallback implements MasterDiscovery.MasterDiscoveryCallback {
        private String masterAddr = "127.0.0.1:10911";
        private String haAddr = "127.0.0.1:10912";
        private boolean failAll = false;
        private int failCount = 0;
        private int callCount = 0;

        void setMasterAddr(String addr) { this.masterAddr = addr; }
        void setHaAddr(String addr) { this.haAddr = addr; }
        void setFailAll(boolean fail) { this.failAll = fail; }
        void setFailCount(int count) { this.failCount = count; }

        @Override
        public Map<String, Map<Long, String>> getBrokerClusterInfo(String namesrvAddr) throws Exception {
            callCount++;
            if (failAll || callCount <= failCount) {
                throw new RuntimeException("模拟 NameServer 查询失败 #" + callCount);
            }
            Map<String, Map<Long, String>> result = new HashMap<>();
            Map<Long, String> brokerAddrs = new HashMap<>();
            brokerAddrs.put(0L, masterAddr);
            result.put("broker-a", brokerAddrs);
            return result;
        }

        @Override
        public String getBrokerHaAddr(String brokerAddr) throws Exception {
            return haAddr;
        }
    }
}
