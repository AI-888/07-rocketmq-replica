package org.apache.rocketmq.hasync.source;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.hasync.config.NameServerAddressUtils;
import org.apache.rocketmq.hasync.util.AclUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocketMQ Master 发现回调实现
 * <p>
 * 使用 RocketMQ Admin API 查询集群信息和 Broker HA 地址
 */
public class RocketMQMasterDiscoveryCallback implements MasterDiscovery.MasterDiscoveryCallback {
    private static final Logger log = LoggerFactory.getLogger(RocketMQMasterDiscoveryCallback.class);
    private final String namesrvAddr;
    private final String ak;
    private final String sk;

    public RocketMQMasterDiscoveryCallback(String namesrvAddr, String ak, String sk) {
        this.namesrvAddr = namesrvAddr;
        this.ak = ak;
        this.sk = sk;
    }

    @Override
    public Map<String, Map<Long, String>> getBrokerClusterInfo() throws Exception {
        if (namesrvAddr == null || namesrvAddr.trim().isEmpty()) {
            throw new RuntimeException("NameServer 地址为空");
        }
        NameServerAddressUtils.setNameServerAddresses(namesrvAddr);
        RPCHook rpcHook = AclUtils.getAclRPCHook(ak, sk);

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook, 3000);
        defaultMQAdminExt.setAdminExtGroup("ha-sync-admin-" + System.currentTimeMillis());
        defaultMQAdminExt.start();

        log.info("通过 RocketMQ Admin API 查询 NameServer: {} 的集群信息", namesrvAddr);

        ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
        Map<String, Map<Long, String>> result = new HashMap<>();
        for (Map.Entry<String, BrokerData> entry : clusterInfo.getBrokerAddrTable().entrySet()) {
            result.put(entry.getKey(), new HashMap<>(entry.getValue().getBrokerAddrs()));
        }

        log.debug("获取集群信息成功: {}", result);
        return result;
    }

    @Override
    public String getBrokerHaAddr(String brokerAddr) throws Exception {
        // 对于 RocketMQ，HA 地址通常是 brokerAddr 的 HA 端口
        // 默认情况下，HA 端口 = broker 端口 + 1
        if (brokerAddr == null || brokerAddr.isEmpty()) {
            throw new RuntimeException("Broker 地址为空");
        }

        String[] parts = brokerAddr.split(":");
        if (parts.length != 2) {
            throw new RuntimeException("无效的 Broker 地址格式: " + brokerAddr);
        }

        try {
            String host = parts[0];
            int brokerPort = Integer.parseInt(parts[1]);
            int haPort = brokerPort + 1; // 默认 HA 端口规则

            String haAddr = host + ":" + haPort;
            log.debug("计算 HA 地址: {} -> {}", brokerAddr, haAddr);
            return haAddr;
        } catch (NumberFormatException e) {
            throw new RuntimeException("无效的 Broker 端口: " + parts[1], e);
        }
    }
}