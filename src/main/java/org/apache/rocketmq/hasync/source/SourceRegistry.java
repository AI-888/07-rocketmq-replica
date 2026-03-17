package org.apache.rocketmq.hasync.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Source ZMQ 地址注册到目标集群 NameServer KV
 * <p>
 * 将 Source 的 ZMQ REP Socket 地址注册到目标集群 NameServer KV 中，
 * Sink 通过 NameServer KV 自动发现 Source 地址。
 * <p>
 * KV 存储结构：
 * <ul>
 *   <li>namespace: SYNC_SOURCE_CONFIG</li>
 *   <li>key: {brokerName}</li>
 *   <li>value: {host}:{zmqPort}:{timestamp}</li>
 * </ul>
 */
public class SourceRegistry {

    private static final Logger log = LoggerFactory.getLogger(SourceRegistry.class);

    /** KV 命名空间 */
    public static final String NAMESPACE = "SYNC_SOURCE_CONFIG";

    /** 默认刷新间隔（毫秒） */
    private static final long DEFAULT_REFRESH_INTERVAL = 30000;

    private final String targetNamesrvAddr;
    private final String brokerName;
    private final String zmqHost;
    private final int zmqPort;
    private final AtomicBoolean registered = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;

    /** KV 操作回调（抽象实际的 NameServer 调用） */
    private RegistryCallback callback;

    /**
     * 注册回调接口
     */
    public interface RegistryCallback {
        /**
         * 写入 KV 配置
         */
        void putKVConfig(String namespace, String key, String value) throws Exception;

        /**
         * 删除 KV 配置
         */
        void deleteKVConfig(String namespace, String key) throws Exception;
    }

    public SourceRegistry(String targetNamesrvAddr, String brokerName, String zmqHost, int zmqPort) {
        this.targetNamesrvAddr = targetNamesrvAddr;
        this.brokerName = brokerName;
        this.zmqHost = zmqHost;
        this.zmqPort = zmqPort;
    }

    public void setCallback(RegistryCallback callback) {
        this.callback = callback;
    }

    /**
     * 注册 Source ZMQ 地址到 NameServer KV
     */
    public void register() throws Exception {
        if (callback == null) {
            throw new IllegalStateException("RegistryCallback 未设置");
        }

        String value = buildRegistryValue();
        callback.putKVConfig(NAMESPACE, brokerName, value);
        registered.set(true);

        log.info("Source ZMQ 地址已注册: namespace={}, key={}, value={}",
                NAMESPACE, brokerName, value);

        // 启动定期刷新
        scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "source-registry-refresh");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::refresh, DEFAULT_REFRESH_INTERVAL,
                DEFAULT_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * 刷新注册信息（更新时间戳）
     */
    public void refresh() {
        try {
            String value = buildRegistryValue();
            callback.putKVConfig(NAMESPACE, brokerName, value);
            log.debug("Source 注册信息已刷新: {}", value);
        } catch (Exception e) {
            log.warn("刷新 Source 注册信息失败: {}", e.getMessage());
        }
    }

    /**
     * 注销（从 NameServer KV 删除注册信息）
     */
    public void unregister() {
        if (!registered.get()) {
            return;
        }

        try {
            if (callback != null) {
                callback.deleteKVConfig(NAMESPACE, brokerName);
                log.info("Source ZMQ 地址已注销: namespace={}, key={}", NAMESPACE, brokerName);
            }
        } catch (Exception e) {
            log.warn("注销 Source 注册信息失败: {}", e.getMessage());
        }

        registered.set(false);

        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    /**
     * 构建注册值
     */
    private String buildRegistryValue() {
        return zmqHost + ":" + zmqPort + ":" + System.currentTimeMillis();
    }

    // ==================== Getters ====================

    public boolean isRegistered() {
        return registered.get();
    }

    public String getTargetNamesrvAddr() {
        return targetNamesrvAddr;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getZmqAddress() {
        return zmqHost + ":" + zmqPort;
    }

    public static String getNamespace() {
        return NAMESPACE;
    }
}
