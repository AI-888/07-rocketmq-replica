package org.apache.rocketmq.hasync.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.alibaba.fastjson2.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * HTTP 监控服务 — 提供 /metrics、/health、/resume 接口
 * <p>
 * 对应需求 20 §5：
 * <ul>
 *   <li>Source 端默认端口 9876，Sink 端默认端口 9877</li>
 *   <li>GET /metrics — 返回全部指标 JSON 快照</li>
 *   <li>GET /health — 健康检查</li>
 *   <li>POST /resume — 恢复暂停状态</li>
 * </ul>
 * <p>
 * 同时按需求 20 §4 每隔 10 秒将全部指标打印到日志。
 */
public class MetricsHttpServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsHttpServer.class);

    private final MetricsCollector metricsCollector;
    private final int port;
    private final String role; // "source" 或 "sink"
    private HttpServer httpServer;
    private ScheduledExecutorService logScheduler;

    /** 暂停恢复回调 */
    private volatile Runnable resumeCallback;

    public MetricsHttpServer(MetricsCollector metricsCollector, int port, String role) {
        this.metricsCollector = metricsCollector;
        this.port = port;
        this.role = role;
    }

    /**
     * 设置暂停恢复回调
     * <p>
     * POST /resume 时调用此回调恢复暂停状态
     */
    public void setResumeCallback(Runnable callback) {
        this.resumeCallback = callback;
    }

    /**
     * 启动 HTTP 监控服务
     */
    public void start() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        httpServer.setExecutor(Executors.newFixedThreadPool(2));

        // GET /metrics
        httpServer.createContext("/metrics", new MetricsHandler());

        // GET /health
        httpServer.createContext("/health", new HealthHandler());

        // POST /resume
        httpServer.createContext("/resume", new ResumeHandler());

        httpServer.start();
        log.info("[{}] HTTP 监控服务启动于端口 {}", role, port);

        // 启动定期日志输出（需求 20 §4 — 每隔 10 秒）
        logScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-log-" + role);
            t.setDaemon(true);
            return t;
        });
        logScheduler.scheduleAtFixedRate(this::logMetrics, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * 停止 HTTP 监控服务
     */
    public void stop() {
        if (logScheduler != null) {
            logScheduler.shutdownNow();
        }
        if (httpServer != null) {
            httpServer.stop(1);
            log.info("[{}] HTTP 监控服务已停止", role);
        }
    }

    /**
     * 获取端口号
     */
    public int getPort() {
        return port;
    }

    /**
     * 定期将指标打印到日志（需求 20 §4）
     */
    private void logMetrics() {
        try {
            Map<String, Object> metrics = metricsCollector.getAllMetrics();
            log.info("[{}] metrics: {}", role, JSON.toJSONString(metrics));
        } catch (Exception e) {
            log.warn("[{}] 指标日志输出异常", role, e);
        }
    }

    // ==================== HTTP Handlers ====================

    /**
     * GET /metrics — 返回全部指标 JSON（需求 20 §5）
     */
    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method Not Allowed\"}");
                return;
            }

            Map<String, Object> response = new LinkedHashMap<>();
            if ("source".equals(role)) {
                response.put("source", metricsCollector.getSourceMetrics());
                response.put("pipeline", metricsCollector.getPipelineMetrics());
            } else {
                response.put("sink", metricsCollector.getSinkMetrics());
            }

            String json = JSON.toJSONString(response);
            sendResponse(exchange, 200, json);
        }
    }

    /**
     * GET /health — 健康检查
     */
    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method Not Allowed\"}");
                return;
            }

            Map<String, Object> health = new LinkedHashMap<>();
            health.put("status", "UP");
            health.put("role", role);
            health.put("connectionStatus", metricsCollector.getConnectionStatus());
            health.put("targetClusterStatus", metricsCollector.getTargetClusterStatus());

            String json = JSON.toJSONString(health);
            sendResponse(exchange, 200, json);
        }
    }

    /**
     * POST /resume — 恢复暂停状态（需求 14 §15、需求 12 §15）
     */
    private class ResumeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method Not Allowed, use POST\"}");
                return;
            }

            Map<String, Object> response = new LinkedHashMap<>();
            if (resumeCallback != null) {
                try {
                    resumeCallback.run();
                    response.put("status", "OK");
                    response.put("message", "已触发恢复操作");
                    log.info("[{}] 收到 /resume 请求，已触发恢复", role);
                    sendResponse(exchange, 200, JSON.toJSONString(response));
                } catch (Exception e) {
                    response.put("status", "ERROR");
                    response.put("message", "恢复失败: " + e.getMessage());
                    log.error("[{}] /resume 恢复失败", role, e);
                    sendResponse(exchange, 500, JSON.toJSONString(response));
                }
            } else {
                response.put("status", "NOOP");
                response.put("message", "当前不处于暂停状态");
                sendResponse(exchange, 200, JSON.toJSONString(response));
            }
        }
    }

    /**
     * 发送 HTTP 响应
     */
    private void sendResponse(HttpExchange exchange, int statusCode, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
