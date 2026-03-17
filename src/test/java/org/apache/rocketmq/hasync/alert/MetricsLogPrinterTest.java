package org.apache.rocketmq.hasync.alert;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * MetricsLogPrinter 单元测试
 */
public class MetricsLogPrinterTest {

    private MetricsCollector metricsCollector;
    private MetricsLogPrinter printer;

    @Before
    public void setUp() {
        metricsCollector = new MetricsCollector();
        printer = new MetricsLogPrinter(metricsCollector, 10000);
    }

    @Test
    public void testPrintMetrics() {
        // 设置一些指标
        metricsCollector.setConnectionStatus("CONNECTED");
        metricsCollector.setCurrentMasterAddr("192.168.1.100:10912");
        metricsCollector.incrementSyncSuccessCount();

        printer.printMetrics(); // 不应抛出异常
    }

    @Test
    public void testGetMetricsJson() {
        metricsCollector.setConnectionStatus("CONNECTED");
        String json = printer.getMetricsJson();

        assertNotNull(json);
        assertTrue(json.contains("CONNECTED"));
        assertTrue(json.contains("source"));
        assertTrue(json.contains("sink"));
        assertTrue(json.contains("pipeline"));
    }

    @Test
    public void testStartAndStop() throws Exception {
        printer.start();
        Thread.sleep(100);
        printer.stop();
        // 不应抛出异常
    }

    @Test
    public void testDefaultInterval() {
        MetricsLogPrinter defaultPrinter = new MetricsLogPrinter(metricsCollector);
        defaultPrinter.printMetrics(); // 不应抛出异常
    }

    @Test
    public void testEmptyMetrics() {
        String json = printer.getMetricsJson();
        assertNotNull(json);
        assertTrue(json.length() > 10);
    }
}
