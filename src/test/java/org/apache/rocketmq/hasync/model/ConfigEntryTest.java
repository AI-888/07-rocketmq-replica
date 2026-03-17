package org.apache.rocketmq.hasync.model;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * ConfigEntry 模型单元测试
 */
public class ConfigEntryTest {

    @Test
    public void testConstructorAndGetters() {
        ConfigEntry entry = new ConfigEntry("192.168.1.1:9876", "CLI");
        assertEquals("192.168.1.1:9876", entry.getValue());
        assertEquals("CLI", entry.getSource());
    }

    @Test
    public void testIsBlankWithNull() {
        ConfigEntry entry = new ConfigEntry(null, "DEFAULT");
        assertTrue("null 值应视为 blank", entry.isBlank());
    }

    @Test
    public void testIsBlankWithEmpty() {
        ConfigEntry entry = new ConfigEntry("", "FILE");
        assertTrue("空字符串应视为 blank", entry.isBlank());
    }

    @Test
    public void testIsBlankWithWhitespace() {
        ConfigEntry entry = new ConfigEntry("   ", "ENV");
        assertTrue("纯空格应视为 blank", entry.isBlank());
    }

    @Test
    public void testIsBlankWithValue() {
        ConfigEntry entry = new ConfigEntry("hello", "CLI");
        assertFalse("非空值不应是 blank", entry.isBlank());
    }

    @Test
    public void testToString() {
        ConfigEntry entry = new ConfigEntry("myValue", "FILE");
        String str = entry.toString();
        assertEquals("myValue [FILE]", str);
    }

    @Test
    public void testDifferentSources() {
        assertEquals("ENV", new ConfigEntry("v", "ENV").getSource());
        assertEquals("CLI", new ConfigEntry("v", "CLI").getSource());
        assertEquals("FILE", new ConfigEntry("v", "FILE").getSource());
        assertEquals("DEFAULT", new ConfigEntry("v", "DEFAULT").getSource());
    }
}
