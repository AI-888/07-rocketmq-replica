package org.apache.rocketmq.hasync.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Sink 进程配置 — ha-sync-sink 的启动参数
 * <p>
 * 对应需求 1 §3-4（Sink 必填/可选参数）
 *
 * @see AbstractConfig
 */
public class SinkConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(SinkConfig.class);

    /** 环境变量前缀 */
    private static final String ENV_PREFIX = "HA_SINK_";

    // ==================== 配置项 Key 常量 ====================

    public static final String TARGET_NAMESRV = "targetNamesrv";
    public static final String SOURCE_NAMESRV = "sourceNamesrv";
    public static final String SINK_METRICS_PORT = "sinkMetricsPort";
    public static final String SINK_ID = "sinkId";
    public static final String SINK_BATCH_SIZE = "sinkBatchSize";
    public static final String SINK_THREADS = "sinkThreads";
    public static final String SINK_MAX_RETRY = "sinkMaxRetry";
    public static final String TARGET_PROBE_INTERVAL = "targetProbeInterval";
    public static final String STARTUP_CHECK_MSG_COUNT = "startupCheckMsgCount";
    public static final String TOPIC_SYNC_MAX_RETRY = "topicSyncMaxRetry";
    public static final String CONFIG_FILE = "configFile";
    public static final String SINK_GRADIENT_MAX_RETRY = "sinkGradientMaxRetry";
    public static final String SINK_GRADIENT_RETRY_DELAYS = "sinkGradientRetryDelays";

    /** 所有配置项 key（有序） */
    private static final Set<String> ALL_KEYS = new LinkedHashSet<>();

    /** 必填配置项 key */
    private static final Set<String> REQUIRED_KEYS = new HashSet<>();

    /** 默认值映射 */
    private static final Map<String, String> DEFAULTS = new HashMap<>();

    static {
        // 注册所有配置项
        ALL_KEYS.add(TARGET_NAMESRV);
        ALL_KEYS.add(SOURCE_NAMESRV);
        ALL_KEYS.add(SINK_METRICS_PORT);
        ALL_KEYS.add(SINK_ID);
        ALL_KEYS.add(SINK_BATCH_SIZE);
        ALL_KEYS.add(SINK_THREADS);
        ALL_KEYS.add(SINK_MAX_RETRY);
        ALL_KEYS.add(TARGET_PROBE_INTERVAL);
        ALL_KEYS.add(STARTUP_CHECK_MSG_COUNT);
        ALL_KEYS.add(TOPIC_SYNC_MAX_RETRY);
        ALL_KEYS.add(CONFIG_FILE);
        ALL_KEYS.add(SINK_GRADIENT_MAX_RETRY);
        ALL_KEYS.add(SINK_GRADIENT_RETRY_DELAYS);

        // 必填参数（需求 1 §3：仅 targetNamesrv 必填）
        // sourceNamesrv 在独立 Sink 模式下需要，但内嵌模式由 SourceBootstrap 自动注入，故不强制必填
        REQUIRED_KEYS.add(TARGET_NAMESRV);

        // 默认值（需求 1 §4）
        DEFAULTS.put(SINK_METRICS_PORT, "9877");
        DEFAULTS.put(SINK_ID, getDefaultSinkId());
        DEFAULTS.put(SINK_BATCH_SIZE, "100");
        DEFAULTS.put(SINK_THREADS, "4");
        DEFAULTS.put(SINK_MAX_RETRY, "3");
        DEFAULTS.put(TARGET_PROBE_INTERVAL, "30000");
        DEFAULTS.put(STARTUP_CHECK_MSG_COUNT, "10");
        DEFAULTS.put(TOPIC_SYNC_MAX_RETRY, "3");
        DEFAULTS.put(SINK_GRADIENT_MAX_RETRY, "5");
        DEFAULTS.put(SINK_GRADIENT_RETRY_DELAYS, "1,3,10,30,60");
    }

    // ==================== 快捷访问方法 ====================

    public String getTargetNamesrv() {
        return getString(TARGET_NAMESRV);
    }

    public String getSourceNamesrv() {
        return getString(SOURCE_NAMESRV);
    }

    public int getSinkMetricsPort() {
        return getInt(SINK_METRICS_PORT, 9877);
    }

    public String getSinkId() {
        return getString(SINK_ID);
    }

    public int getSinkBatchSize() {
        return getInt(SINK_BATCH_SIZE, 100);
    }

    public int getSinkThreads() {
        return getInt(SINK_THREADS, 4);
    }

    public int getSinkMaxRetry() {
        return getInt(SINK_MAX_RETRY, 3);
    }

    public long getTargetProbeInterval() {
        return getLong(TARGET_PROBE_INTERVAL, 30000L);
    }

    public int getStartupCheckMsgCount() {
        return getInt(STARTUP_CHECK_MSG_COUNT, 10);
    }

    public int getTopicSyncMaxRetry() {
        return getInt(TOPIC_SYNC_MAX_RETRY, 3);
    }

    /**
     * 获取梯度重试最大次数（需求 21 §21.3）
     */
    public int getSinkGradientMaxRetry() {
        return getInt(SINK_GRADIENT_MAX_RETRY, 5);
    }

    /**
     * 获取梯度重试各级等待时间字符串（需求 21 §21.3）
     */
    public String getSinkGradientRetryDelays() {
        return getString(SINK_GRADIENT_RETRY_DELAYS);
    }

    // ==================== 抽象方法实现 ====================

    @Override
    protected Set<String> getAllConfigKeys() {
        return ALL_KEYS;
    }

    @Override
    protected Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    @Override
    protected String getDefaultValue(String key) {
        return DEFAULTS.get(key);
    }

    /**
     * 环境变量命名规则（需求 1 §9）：
     * targetNamesrv → HA_SINK_TARGET_NAMESRV
     * sinkBatchSize → HA_SINK_SINK_BATCH_SIZE
     */
    @Override
    protected String toEnvName(String key) {
        return ENV_PREFIX + camelToUpperSnake(key);
    }

    @Override
    protected String getDefaultConfigFilePath() {
        return "./ha-sync-sink.properties";
    }

    @Override
    protected void printUsage() {
        System.err.println("用法: java -jar ha-sync.jar --mode sink [选项]");
        System.err.println();
        System.err.println("必填参数:");
        System.err.println("  --targetNamesrv <addr>    目标集群 NameServer 地址");
        System.err.println();
        System.err.println("可选参数:");
        System.err.println("  --sourceNamesrv <addr>                源集群 NameServer 地址（独立模式下用于发现 Source ZMQ 地址，内嵌模式自动注入）");
        System.err.println("  --sinkMetricsPort <port>              HTTP 监控端口（默认: 9877）");
        System.err.println("  --sinkId <id>                         Sink 节点唯一标识（默认: hostname:pid）");
        System.err.println("  --sinkBatchSize <n>                   批量发送大小（默认: 100）");
        System.err.println("  --sinkThreads <n>                     并发写入线程数（默认: 4）");
        System.err.println("  --sinkMaxRetry <n>                    写入失败最大重试次数（默认: 3）");
        System.err.println("  --targetProbeInterval <ms>            目标集群探活间隔（默认: 30000）");
        System.err.println("  --startupCheckMsgCount <n>            启动校验消息条数（默认: 10，0=跳过）");
        System.err.println("  --topicSyncMaxRetry <n>               Topic 同步最大重试次数（默认: 3）");
        System.err.println("  --sinkGradientMaxRetry <n>            梯度重试最大次数（默认: 5）（需求 21 §21.3）");
        System.err.println("  --sinkGradientRetryDelays <delays>    各级重试等待时间，逗号分隔秒数（默认: 1,3,10,30,60）");
        System.err.println("  --configFile <path>                   配置文件路径（默认: ./ha-sync-sink.properties）");
    }

    // ==================== 工具方法 ====================

    /**
     * camelCase → UPPER_SNAKE_CASE
     * 例：targetNamesrv → TARGET_NAMESRV
     *     sinkBatchSize → SINK_BATCH_SIZE
     */
    public static String camelToUpperSnake(String camelCase) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c)) {
                if (sb.length() > 0) {
                    sb.append('_');
                }
                sb.append(c);
            } else {
                sb.append(Character.toUpperCase(c));
            }
        }
        return sb.toString();
    }

    /**
     * 生成默认 Sink ID：hostname:pid
     */
    private static String getDefaultSinkId() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            return hostname + ":" + pid;
        } catch (Exception e) {
            return "unknown:" + System.currentTimeMillis();
        }
    }
}