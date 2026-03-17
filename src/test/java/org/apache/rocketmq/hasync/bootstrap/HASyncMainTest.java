package org.apache.rocketmq.hasync.bootstrap;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * HASyncMain 统一入口单元测试
 */
public class HASyncMainTest {

    // ==================== extractMode 测试 ====================

    @Test
    public void testExtractMode_source() {
        String[] args = {"--mode", "source", "--sourceNamesrv", "127.0.0.1:9876"};
        assertEquals("source", HASyncMain.extractMode(args));
    }

    @Test
    public void testExtractMode_sink() {
        String[] args = {"--mode", "sink", "--targetNamesrv", "127.0.0.1:9877"};
        assertEquals("sink", HASyncMain.extractMode(args));
    }

    @Test
    public void testExtractMode_null() {
        assertNull(HASyncMain.extractMode(null));
    }

    @Test
    public void testExtractMode_emptyArgs() {
        assertNull(HASyncMain.extractMode(new String[]{}));
    }

    @Test
    public void testExtractMode_noModeArg() {
        String[] args = {"--sourceNamesrv", "127.0.0.1:9876"};
        assertNull(HASyncMain.extractMode(args));
    }

    @Test
    public void testExtractMode_modeMissesValue() {
        String[] args = {"--mode"};
        assertNull(HASyncMain.extractMode(args));
    }

    @Test
    public void testExtractMode_modeInMiddle() {
        String[] args = {"--sourceNamesrv", "127.0.0.1:9876", "--mode", "source", "--targetNamesrv", "127.0.0.1:9877"};
        assertEquals("source", HASyncMain.extractMode(args));
    }

    // ==================== stripModeArgs 测试 ====================

    @Test
    public void testStripModeArgs_normal() {
        String[] args = {"--mode", "source", "--sourceNamesrv", "127.0.0.1:9876"};
        String[] result = HASyncMain.stripModeArgs(args);
        assertEquals(2, result.length);
        assertEquals("--sourceNamesrv", result[0]);
        assertEquals("127.0.0.1:9876", result[1]);
    }

    @Test
    public void testStripModeArgs_null() {
        String[] result = HASyncMain.stripModeArgs(null);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    public void testStripModeArgs_noMode() {
        String[] args = {"--sourceNamesrv", "127.0.0.1:9876", "--targetNamesrv", "127.0.0.1:9877"};
        String[] result = HASyncMain.stripModeArgs(args);
        assertEquals(4, result.length);
    }

    @Test
    public void testStripModeArgs_modeOnly() {
        String[] args = {"--mode", "source"};
        String[] result = HASyncMain.stripModeArgs(args);
        assertEquals(0, result.length);
    }

    @Test
    public void testStripModeArgs_modeInMiddle() {
        String[] args = {"--a", "1", "--mode", "sink", "--b", "2"};
        String[] result = HASyncMain.stripModeArgs(args);
        assertEquals(4, result.length);
        assertEquals("--a", result[0]);
        assertEquals("1", result[1]);
        assertEquals("--b", result[2]);
        assertEquals("2", result[3]);
    }

    @Test
    public void testStripModeArgs_emptyArgs() {
        String[] result = HASyncMain.stripModeArgs(new String[]{});
        assertEquals(0, result.length);
    }
}
