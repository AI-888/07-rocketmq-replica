package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.source.SourceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Source ZMQ 地址发现 — Sink 端通过源集群 NameServer KV 查找所有 Source 的 ZMQ 地址
 * <p>
 * 设计说明：
 * <ul>
 *   <li>每个 Source 以 sourceNodeId 为 key，将 ZMQ 地址注册到源集群 NameServer KV（namespace: SYNC_SOURCE_CONFIG）</li>
 *   <li>Sink 启动时扫描该 namespace 下所有 KV，获取全部 Source 的 ZMQ 地址列表</li>
 *   <li>Sink 连接其中一个（或多个）Source 进行数据拉取</li>
 * </ul>
 */
public class SourceDiscovery {

    private static final Logger log = LoggerFactory.getLogger(SourceDiscovery.class);

    private final String sourceNamesrvAddr;

    /** KV 查询回调（抽象实际的 NameServer 调用） */
    private DiscoveryCallback callback;

    /**
     * KV 查询回调接口
     */
    public interface DiscoveryCallback {
        /**
         * 获取指定 namespace 下所有 KV 对
         *
         * @param namespace KV 命名空间
         * @return key → value 映射
         */
        Map<String, String> getKVListByNamespace(String namespace) throws Exception;
    }

    public SourceDiscovery(String sourceNamesrvAddr) {
        this.sourceNamesrvAddr = sourceNamesrvAddr;
    }

    public void setCallback(DiscoveryCallback callback) {
        this.callback = callback;
    }

    /**
     * 发现所有 Source 的 ZMQ 地址
     * <p>
     * 从源集群 NameServer KV 中扫描 SYNC_SOURCE_CONFIG namespace，
     * 解析每个 Source 注册的 ZMQ 地址（格式: host:port:timestamp）。
     *
     * @return Source ZMQ 地址列表（格式: host:port），空列表表示未发现
     */
    public List<String> discoverSourceAddresses() {
        if (callback == null) {
            log.warn("DiscoveryCallback 未设置，无法发现 Source 地址");
            return Collections.emptyList();
        }

        try {
            Map<String, String> kvMap = callback.getKVListByNamespace(SourceRegistry.NAMESPACE);
            if (kvMap == null || kvMap.isEmpty()) {
                log.warn("未在源集群 NameServer 中发现任何 Source 注册信息（namespace={}）",
                        SourceRegistry.NAMESPACE);
                return Collections.emptyList();
            }

            List<String> addresses = new ArrayList<>();
            for (Map.Entry<String, String> entry : kvMap.entrySet()) {
                String sourceNodeId = entry.getKey();
                String registryValue = entry.getValue();

                // 格式: host:port:timestamp
                String zmqAddress = parseZmqAddress(registryValue);
                if (zmqAddress != null) {
                    addresses.add(zmqAddress);
                    log.info("发现 Source: nodeId={}, zmqAddress={}", sourceNodeId, zmqAddress);
                } else {
                    log.warn("Source 注册信息格式异常: nodeId={}, value={}", sourceNodeId, registryValue);
                }
            }

            log.info("共发现 {} 个 Source ZMQ 地址", addresses.size());
            return addresses;

        } catch (Exception e) {
            log.error("从源集群 NameServer 查询 Source 地址失败: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    /**
     * 发现第一个可用的 Source ZMQ 地址
     *
     * @return Source ZMQ 地址（host:port），null 表示未发现
     */
    public String discoverFirstSourceAddress() {
        List<String> addresses = discoverSourceAddresses();
        if (addresses.isEmpty()) {
            return null;
        }
        return addresses.get(0);
    }

    /**
     * 解析注册值中的 ZMQ 地址
     * <p>
     * 注册值格式: host:port:timestamp → 提取 host:port
     *
     * @param registryValue 注册值
     * @return ZMQ 地址（host:port），解析失败返回 null
     */
    static String parseZmqAddress(String registryValue) {
        if (registryValue == null || registryValue.isEmpty()) {
            return null;
        }

        // 格式: host:port:timestamp，从右往左找最后一个冒号分隔timestamp
        int lastColon = registryValue.lastIndexOf(':');
        if (lastColon <= 0) {
            return null;
        }

        String withoutTimestamp = registryValue.substring(0, lastColon);
        // withoutTimestamp 应为 host:port
        int portColon = withoutTimestamp.lastIndexOf(':');
        if (portColon <= 0) {
            return null;
        }

        try {
            String host = withoutTimestamp.substring(0, portColon);
            int port = Integer.parseInt(withoutTimestamp.substring(portColon + 1));
            if (port > 0 && port <= 65535 && !host.isEmpty()) {
                return host + ":" + port;
            }
        } catch (NumberFormatException e) {
            // 忽略
        }
        return null;
    }

    // ==================== Getters ====================

    public String getSourceNamesrvAddr() {
        return sourceNamesrvAddr;
    }
}
