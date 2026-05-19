package org.apache.rocketmq.hasync.source;

import com.alibaba.fastjson2.JSON;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NameServer Master 地址发现
 * <p>
 * 通过 NameServer 的 GET_BROKER_CLUSTER_INFO 接口查询集群信息，
 * 识别 brokerId=0 的节点为 Master，获取其 HA 服务地址。
 * <p>
 * 支持指数退避重试（初始 1s，最大 30s）。
 *
 * @see <a href="https://github.com/apache/rocketmq">NameServer</a>
 */
public class MasterDiscovery {

    private static final Logger log = LoggerFactory.getLogger(MasterDiscovery.class);

    /**
     * 初始重试间隔（毫秒）
     */
    private static final long INITIAL_RETRY_INTERVAL = 1000;

    /**
     * 最大重试间隔（毫秒）
     */
    private static final long MAX_RETRY_INTERVAL = 30000;

    /**
     * NameServer 地址（多个以 ; 分隔）
     */
    private final String namesrvAddr;

    /**
     * Broker 名称（用于筛选特定 Broker 组）
     */
    private final String brokerName;

    /**
     * NameServer 查询失败计数
     */
    private final AtomicInteger nameSrvQueryErrorCount = new AtomicInteger(0);

    /**
     * 当前已知的 Master HA 地址
     */
    private volatile String currentMasterHaAddr;

    /**
     * 当前已知的 Master Broker 地址（用于 Admin API）
     */
    private volatile String currentMasterBrokerAddr;

    /**
     * Master 切换计数
     */
    private final AtomicInteger masterSwitchCount = new AtomicInteger(0);

    /**
     * NameServer 客户端实例（延迟注入）
     */
    private MasterDiscoveryCallback callback;

    /**
     * Master 发现回调接口
     * <p>
     * 将实际的 RocketMQ Admin API 调用抽象为回调，
     * 便于单元测试中 Mock。
     */
    public interface MasterDiscoveryCallback {
        /**
         * 查询集群中的 Broker 信息
         *
         * @return 集群信息映射：brokerName → {brokerId → brokerAddr}
         */
        Map<String, Map<Long, String>> getBrokerClusterInfo() throws Exception;

        /**
         * 获取指定 Broker 的 HA 服务地址
         *
         * @param brokerAddr Broker 管理地址
         * @return HA 地址（host:haPort）
         */
        String getBrokerHaAddr(String brokerAddr) throws Exception;
    }

    public MasterDiscovery(String namesrvAddr, String brokerName) {
        this.namesrvAddr = namesrvAddr;
        this.brokerName = brokerName;
    }

    public MasterDiscovery(String namesrvAddr, String brokerName, MasterDiscoveryCallback callback) {
        this.namesrvAddr = namesrvAddr;
        this.brokerName = brokerName;
        this.callback = callback;
    }

    public void setCallback(MasterDiscoveryCallback callback) {
        this.callback = callback;
    }

    /**
     * 发现 Master HA 地址
     * <p>
     * 通过 NameServer 查询 ClusterInfo，选择 brokerId=0 的节点。
     * 失败时按指数退避重试。
     *
     * @return Master HA 地址（host:port）
     * @throws Exception 查询失败且重试耗尽
     */
    public String discoverMasterHaAddr() throws Exception {
        return discoverMasterHaAddr(5);
    }

    /**
     * 发现 Master HA 地址（带最大重试次数）
     */
    public String discoverMasterHaAddr(int maxRetries) throws Exception {
        long retryInterval = INITIAL_RETRY_INTERVAL;
        Exception lastException = null;

        for (int i = 0; i <= maxRetries; i++) {
            try {
                String haAddr = doDiscover();
                if (haAddr != null) {
                    // 检测 Master 切换
                    if (currentMasterHaAddr != null && !currentMasterHaAddr.equals(haAddr)) {
                        log.info("检测到 Master 切换: {} → {}", currentMasterHaAddr, haAddr);
                        masterSwitchCount.incrementAndGet();
                    }
                    currentMasterHaAddr = haAddr;
                    return haAddr;
                }
            } catch (Exception e) {
                lastException = e;
                nameSrvQueryErrorCount.incrementAndGet();
                if (i < maxRetries) {
                    log.warn("NameServer 查询 Master 地址失败（第 {} 次），{}ms 后重试: {}",
                            i + 1, retryInterval, e.getMessage());
                    Thread.sleep(retryInterval);
                    retryInterval = Math.min(retryInterval * 2, MAX_RETRY_INTERVAL);
                }
            }
        }

        throw new RuntimeException("发现 Master HA 地址失败（重试 " + maxRetries + " 次后放弃）",
                lastException);
    }

    /**
     * 检测 Master 是否发生变更
     *
     * @return true 表示 Master 发生了变更
     */
    public boolean checkMasterChanged() {
        try {
            String newHaAddr = doDiscover();
            if (newHaAddr != null && !newHaAddr.equals(currentMasterHaAddr)) {
                log.info("定时检测到 Master 变更: {} → {} (切换时间: {})",
                        currentMasterHaAddr, newHaAddr, System.currentTimeMillis());
                String oldAddr = currentMasterHaAddr;
                currentMasterHaAddr = newHaAddr;
                masterSwitchCount.incrementAndGet();
                return true;
            }
        } catch (Exception e) {
            nameSrvQueryErrorCount.incrementAndGet();
            log.warn("定时检测 Master 变更失败: {}", e.getMessage());
        }
        return false;
    }

    /**
     * 实际执行 Master 发现
     */
    private String doDiscover() throws Exception {
        if (callback == null) {
            throw new IllegalStateException("MasterDiscoveryCallback 未设置");
        }

        // 1. 查询集群 Broker 信息
        Map<String, Map<Long, String>> clusterInfo = callback.getBrokerClusterInfo();

        if (clusterInfo == null || clusterInfo.isEmpty()) {
            throw new RuntimeException("集群信息为空");
        }

        // 2. 查找目标 Broker 组
        Map<Long, String> brokerAddrs;
        if (brokerName != null && !brokerName.isEmpty()) {
            brokerAddrs = clusterInfo.get(brokerName);
            if (brokerAddrs == null) {
                log.error("未找到 Broker 组: {}, cluster: {}", brokerName, JSON.toJSONString(clusterInfo));
                throw new RuntimeException("未找到 Broker 组: " + brokerName);
            }
        } else {
            // 默认选取第一个 Broker 组
            brokerAddrs = clusterInfo.values().iterator().next();
        }

        // 3. 选择 brokerId=0（Master）
        String masterAddr = brokerAddrs.get(0L);
        if (masterAddr == null) {
            log.error("未找到 Broker 组: {}, cluster: {}", brokerName, JSON.toJSONString(clusterInfo));
            throw new RuntimeException("Broker 组中未找到 brokerId=0 的 Master 节点");
        }

        currentMasterBrokerAddr = masterAddr;

        // 4. 获取 Master 的 HA 地址
        return callback.getBrokerHaAddr(masterAddr);
    }

    // ==================== Getters ====================

    public String getCurrentMasterHaAddr() {
        return currentMasterHaAddr;
    }

    public String getCurrentMasterBrokerAddr() {
        return currentMasterBrokerAddr;
    }

    public int getNameSrvQueryErrorCount() {
        return nameSrvQueryErrorCount.get();
    }

    public int getMasterSwitchCount() {
        return masterSwitchCount.get();
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public String getBrokerName() {
        return brokerName;
    }
}
