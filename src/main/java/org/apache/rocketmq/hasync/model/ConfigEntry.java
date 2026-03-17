package org.apache.rocketmq.hasync.model;

/**
 * 配置项条目 — 记录配置值及其来源
 * <p>
 * 在启动日志中打印最终生效配置及来源（需求 1 §11）
 */
public class ConfigEntry {

    /** 配置值 */
    private final String value;

    /** 配置来源: ENV / CLI / FILE / DEFAULT */
    private final String source;

    public ConfigEntry(String value, String source) {
        this.value = value;
        this.source = source;
    }

    public String getValue() {
        return value;
    }

    public String getSource() {
        return source;
    }

    /**
     * 判断值是否为空或空白
     */
    public boolean isBlank() {
        return value == null || value.trim().isEmpty();
    }

    @Override
    public String toString() {
        return value + " [" + source + "]";
    }
}
