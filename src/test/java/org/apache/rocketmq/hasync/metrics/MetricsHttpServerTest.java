package org.apache.rocketmq.hasync.metrics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * MetricsHttpServer 单元测试
 */
public class MetricsHttpServerTest {

    private MetricsCollector collector;
    private MetricsHttpServer server;
    private int testPort;

    @Before
    public void setUp() throws Exception {
        collector = new MetricsCollector();
        // 使用随机高端口避免冲突
        testPort = 19800 + (int) (Math.random() * 100);
        server = new MetricsHttpServer(collector, testPort, "source");
        server.start();
        // 等待服务器启动
        Thread.sleep(500);
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    // ==================== /metrics 接口 ====================

    @Test
    public void testMetricsEndpoint() throws Exception {
        collector.setConnectionStatus("CONNECTED");
        collector.setCurrentMasterAddr("127.0.0.1:10912");
        collector.incrementConnectionErrorCount();

        String response = httpGet("http://127.0.0.1:" + testPort + "/metrics");
        assertNotNull(response);
        assertTrue("应包含 source 字段", response.contains("source"));
        assertTrue("应包含 pipeline 字段", response.contains("pipeline"));
        assertTrue("应包含 CONNECTED", response.contains("CONNECTED"));
        assertTrue("应包含 connectionErrorCount", response.contains("connectionErrorCount"));
    }

    @Test
    public void testMetricsEndpointSinkRole() throws Exception {
        // 创建 Sink 角色的 server
        int sinkPort = testPort + 1;
        MetricsHttpServer sinkServer = new MetricsHttpServer(collector, sinkPort, "sink");
        try {
            sinkServer.start();
            Thread.sleep(300);

            collector.incrementSyncSuccessCount();
            String response = httpGet("http://127.0.0.1:" + sinkPort + "/metrics");
            assertNotNull(response);
            assertTrue("Sink 角色应包含 sink 字段", response.contains("sink"));
            // Sink 角色不应返回 source 字段
            assertFalse("Sink 角色不应包含 source 字段",
                    response.contains("\"source\""));
        } finally {
            sinkServer.stop();
        }
    }

    // ==================== /health 接口 ====================

    @Test
    public void testHealthEndpoint() throws Exception {
        collector.setConnectionStatus("CONNECTED");
        collector.setTargetClusterStatus("AVAILABLE");

        String response = httpGet("http://127.0.0.1:" + testPort + "/health");
        assertNotNull(response);
        assertTrue("应包含 UP", response.contains("UP"));
        assertTrue("应包含 role", response.contains("source"));
        assertTrue("应包含 CONNECTED", response.contains("CONNECTED"));
        assertTrue("应包含 AVAILABLE", response.contains("AVAILABLE"));
    }

    // ==================== /resume 接口 ====================

    @Test
    public void testResumeEndpointWithCallback() throws Exception {
        final boolean[] called = {false};
        server.setResumeCallback(() -> called[0] = true);

        String response = httpPost("http://127.0.0.1:" + testPort + "/resume");
        assertNotNull(response);
        assertTrue("应包含 OK", response.contains("OK"));
        assertTrue("回调应被调用", called[0]);
    }

    @Test
    public void testResumeEndpointWithoutCallback() throws Exception {
        // 不设置回调
        String response = httpPost("http://127.0.0.1:" + testPort + "/resume");
        assertNotNull(response);
        assertTrue("应包含 NOOP", response.contains("NOOP"));
    }

    @Test
    public void testResumeEndpointMethodNotAllowed() throws Exception {
        // 用 GET 请求 /resume 应该返回 405
        int statusCode = httpGetStatusCode("http://127.0.0.1:" + testPort + "/resume");
        assertEquals(405, statusCode);
    }

    // ==================== 端口测试 ====================

    @Test
    public void testGetPort() {
        assertEquals(testPort, server.getPort());
    }

    // ==================== 辅助方法 ====================

    private String httpGet(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);

        int status = conn.getResponseCode();
        assertEquals(200, status);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } finally {
            conn.disconnect();
        }
    }

    private int httpGetStatusCode(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);
        int status = conn.getResponseCode();
        conn.disconnect();
        return status;
    }

    private String httpPost(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);
        conn.setDoOutput(true);
        conn.getOutputStream().write(new byte[0]);
        conn.getOutputStream().flush();

        int status = conn.getResponseCode();
        assertEquals(200, status);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } finally {
            conn.disconnect();
        }
    }
}
