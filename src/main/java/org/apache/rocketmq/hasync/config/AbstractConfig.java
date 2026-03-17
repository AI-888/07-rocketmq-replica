package org.apache.rocketmq.hasync.config;

import org.apache.rocketmq.hasync.model.ConfigEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 配置管理抽象基类 — 三层配置合并加载
 * <p>
 * 优先级（高 → 低）：环境变量 > 命令行参数 > 配置文件 > 默认值
 * <p>
 * 对应需求 1（启动参数配置）
 *
 * @see org.apache.rocketmq.hasync.config.SourceConfig
 * @see org.apache.rocketmq.hasync.config.SinkConfig
 */
public abstract class AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(AbstractConfig.class);

    /** 最终生效的配置项 */
    protected final Map<String, ConfigEntry> configMap = new LinkedHashMap<>();

    /**
     * 三层配置合并加载
     * <p>
     * 加载流程（需求 1 §5-12）：
     * <ol>
     *   <li>加载配置文件（最低优先级）</li>
     *   <li>解析命令行参数（中优先级，覆盖配置文件）</li>
     *   <li>读取环境变量（最高优先级，覆盖所有）</li>
     *   <li>填充默认值</li>
     *   <li>校验必填参数</li>
     *   <li>打印最终生效配置</li>
     * </ol>
     *
     * @param args 命令行参数
     * @return 配置项与来源的映射
     */
    public Map<String, ConfigEntry> load(String[] args) {
        configMap.clear();

        // 1. 加载配置文件（最低优先级）
        String configFilePath = extractConfigFilePath(args);
        loadFromFile(configFilePath);

        // 2. 解析命令行参数（中优先级，覆盖配置文件）
        loadFromCLI(args);

        // 3. 读取环境变量（最高优先级，覆盖所有）
        loadFromEnv();

        // 4. 填充默认值
        fillDefaults();

        // 5. 校验必填参数
        validateRequired();

        // 6. 打印最终生效配置（敏感信息掩码 — 需求 1 §11）
        logFinalConfig();

        return configMap;
    }

    // ==================== 配置文件加载 ====================

    /**
     * 从配置文件加载（需求 1 §5-8）
     */
    private void loadFromFile(String configFilePath) {
        if (configFilePath == null || configFilePath.isEmpty()) {
            configFilePath = getDefaultConfigFilePath();
        }

        Properties fileProps = new Properties();
        try (FileInputStream fis = new FileInputStream(configFilePath)) {
            fileProps.load(fis);
            log.info("成功加载配置文件: {}", configFilePath);
        } catch (IOException e) {
            // 配置文件不存在 → 忽略（需求 1 §5）
            log.info("配置文件不存在或无法读取: {}，将使用命令行参数和环境变量", configFilePath);
            return;
        } catch (Exception e) {
            // 配置文件格式错误 → ERROR 日志并退出（需求 1 §8）
            log.error("配置文件解析失败: {}，错误原因: {}", configFilePath, e.getMessage());
            System.exit(1);
        }

        Set<String> knownKeys = getAllConfigKeys();
        for (String key : fileProps.stringPropertyNames()) {
            if (!knownKeys.contains(key)) {
                // 未识别的配置项 → WARN 日志并忽略（需求 1 §7）
                log.warn("未识别的配置项: {}，已忽略", key);
                continue;
            }
            String value = fileProps.getProperty(key);
            // 空字符串视为未设置（需求 1 §12）
            if (value != null && !value.trim().isEmpty()) {
                configMap.put(key, new ConfigEntry(value.trim(), "FILE"));
            }
        }
    }

    // ==================== 命令行参数解析 ====================

    /**
     * 从命令行参数加载（覆盖配置文件中的同名项）
     */
    private void loadFromCLI(String[] args) {
        Map<String, String> cliArgs = parseCLI(args);
        for (Map.Entry<String, String> entry : cliArgs.entrySet()) {
            String value = entry.getValue();
            // 空字符串视为未设置（需求 1 §12）
            if (value != null && !value.trim().isEmpty()) {
                configMap.put(entry.getKey(), new ConfigEntry(value.trim(), "CLI"));
            }
        }
    }

    /**
     * 解析命令行参数 --key value 格式
     */
    protected Map<String, String> parseCLI(String[] args) {
        Map<String, String> result = new HashMap<>();
        if (args == null) {
            return result;
        }

        Set<String> knownKeys = getAllConfigKeys();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length) {
                String key = args[i].substring(2);
                if (knownKeys.contains(key)) {
                    String value = args[i + 1];
                    result.put(key, value);
                    i++; // 跳过 value
                }
            }
        }
        return result;
    }

    // ==================== 环境变量加载 ====================

    /**
     * 从环境变量加载（最高优先级，覆盖所有 — 需求 1 §9-10）
     */
    private void loadFromEnv() {
        for (String key : getAllConfigKeys()) {
            String envName = toEnvName(key);
            String envValue = System.getenv(envName);
            if (envValue != null && !envValue.trim().isEmpty()) {
                configMap.put(key, new ConfigEntry(envValue.trim(), "ENV"));
            }
        }
    }

    // ==================== 默认值填充 ====================

    /**
     * 填充默认值（需求 1 §2、§4）
     */
    private void fillDefaults() {
        for (String key : getAllConfigKeys()) {
            if (!configMap.containsKey(key) || configMap.get(key).isBlank()) {
                String defaultValue = getDefaultValue(key);
                if (defaultValue != null) {
                    configMap.put(key, new ConfigEntry(defaultValue, "DEFAULT"));
                }
            }
        }
    }

    // ==================== 参数校验 ====================

    /**
     * 校验必填参数（需求 1 §13）
     */
    private void validateRequired() {
        List<String> missing = new ArrayList<>();
        for (String key : getRequiredKeys()) {
            if (!configMap.containsKey(key) || configMap.get(key).isBlank()) {
                missing.add(key);
            }
        }

        if (!missing.isEmpty()) {
            log.error("缺少必填参数: {}", missing);
            printUsage();
            System.exit(1);
        }
    }

    // ==================== 日志打印 ====================

    /**
     * 打印最终生效配置及来源（需求 1 §11）
     * 敏感信息使用掩码
     */
    private void logFinalConfig() {
        log.info("========== 最终生效配置 ==========");
        for (Map.Entry<String, ConfigEntry> entry : configMap.entrySet()) {
            String key = entry.getKey();
            ConfigEntry ce = entry.getValue();
            String displayValue = maskSensitive(key, ce.getValue());
            log.info("  {} = {} [{}]", key, displayValue, ce.getSource());
        }
        log.info("==================================");
    }

    // ==================== 工具方法 ====================

    /**
     * 从命令行参数中提取 --configFile 的值
     */
    protected String extractConfigFilePath(String[] args) {
        if (args == null) {
            return null;
        }
        for (int i = 0; i < args.length; i++) {
            if ("--configFile".equals(args[i]) && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return null;
    }

    /**
     * 获取配置项的字符串值
     *
     * @param key 配置项 key
     * @return 值，如果不存在则返回 null
     */
    public String getString(String key) {
        ConfigEntry entry = configMap.get(key);
        return entry != null ? entry.getValue() : null;
    }

    /**
     * 获取配置项的整数值
     *
     * @param key 配置项 key
     * @param defaultValue 默认值
     * @return 整数值
     */
    public int getInt(String key, int defaultValue) {
        String value = getString(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("配置项 {} 值 '{}' 不是合法整数，使用默认值 {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 获取配置项的整数值（无默认值，返回 0）
     */
    public int getInt(String key) {
        return getInt(key, 0);
    }

    /**
     * 获取配置项的长整数值
     *
     * @param key 配置项 key
     * @param defaultValue 默认值
     * @return 长整数值
     */
    public long getLong(String key, long defaultValue) {
        String value = getString(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            log.warn("配置项 {} 值 '{}' 不是合法长整数，使用默认值 {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 敏感信息掩码（需求 1 §11）
     * <p>
     * NameServer 地址 → 192.168.*.***:9876
     */
    public String maskSensitive(String key, String value) {
        if (key.toLowerCase().contains("namesrv")) {
            return value.replaceAll("(\\d+\\.\\d+\\.)\\d+\\.\\d+", "$1*.***");
        }
        return value;
    }

    // ==================== 抽象方法 ====================

    /**
     * 获取所有已知的配置项 key 集合
     */
    protected abstract Set<String> getAllConfigKeys();

    /**
     * 获取所有必填配置项 key 集合
     */
    protected abstract Set<String> getRequiredKeys();

    /**
     * 获取配置项的默认值（如果有）
     *
     * @param key 配置项 key
     * @return 默认值，null 表示无默认值
     */
    protected abstract String getDefaultValue(String key);

    /**
     * 将配置项名称转换为环境变量名称（需求 1 §9）
     * <p>
     * 示例：
     * <ul>
     *   <li>Source: sourceNamesrv → HA_SOURCE_SOURCE_NAMESRV</li>
     *   <li>Sink: targetNamesrv → HA_SINK_TARGET_NAMESRV</li>
     * </ul>
     *
     * @param key 配置项 key（camelCase）
     * @return 环境变量名（UPPER_SNAKE_CASE）
     */
    protected abstract String toEnvName(String key);

    /**
     * 获取默认配置文件路径
     */
    protected abstract String getDefaultConfigFilePath();

    /**
     * 打印使用说明
     */
    protected abstract void printUsage();
}
