package org.apache.rocketmq.hasync.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * RocketMQ 测试集群生命周期管理器
 * <p>
 * 基于 Docker Compose 启停测试集群，提供：
 * <ul>
 *   <li>{@link #startCluster()} — docker compose up -d + 等待健康检查</li>
 *   <li>{@link #stopCluster()} — docker compose down -v</li>
 *   <li>{@link #writeClusterConfig()} — 写入集群连接信息到本地文件</li>
 *   <li>{@link #waitForReady()} — 轮询直到集群完全就绪</li>
 *   <li>{@link #loadClusterConfig()} — 从本地文件读取集群信息</li>
 * </ul>
 */
public class RocketMQClusterManager {

    private static final Logger log = LoggerFactory.getLogger(RocketMQClusterManager.class);

    /** 集群配置文件路径 */
    public static final String CONFIG_FILE = "target/test-cluster.properties";

    /** Docker Compose 文件路径（相对项目根目录） */
    public static final String COMPOSE_FILE = "docker-compose.yml";

    /** 项目根目录（自动探测） */
    private final String projectRoot;

    /** NameServer 地址列表 */
    public static final String NAMESRV_ADDR = "127.0.0.1:9876;127.0.0.1:9877;127.0.0.1:9878";

    /** 源集群名 */
    public static final String SOURCE_CLUSTER_NAME = "SourceCluster";

    /** 目标集群名 */
    public static final String SINK_CLUSTER_NAME = "SinkCluster";

    /** Dashboard URL */
    public static final String DASHBOARD_URL = "http://127.0.0.1:8080";

    /** 集群就绪等待超时（秒） */
    private static final int READY_TIMEOUT_SECONDS = 120;

    /** 轮询间隔（秒） */
    private static final int POLL_INTERVAL_SECONDS = 3;

    public RocketMQClusterManager() {
        this.projectRoot = detectProjectRoot();
    }

    public RocketMQClusterManager(String projectRoot) {
        this.projectRoot = projectRoot;
    }

    /**
     * 启动测试集群
     * <p>
     * 执行 docker compose up -d，等待所有服务健康，然后写入配置文件。
     *
     * @return true 启动成功
     * @throws Exception 启动失败
     */
    public boolean startCluster() throws Exception {
        log.info("========== 启动 RocketMQ 测试集群 ==========");

        // 检查 Docker Compose 文件是否存在
        File composeFile = new File(projectRoot, COMPOSE_FILE);
        if (!composeFile.exists()) {
            throw new FileNotFoundException("Docker Compose 文件不存在: " + composeFile.getAbsolutePath());
        }

        // 启动集群
        log.info("执行 docker compose up -d ...");
        int exitCode = execCommand(projectRoot, "docker", "compose", "-f", COMPOSE_FILE, "up", "-d");
        if (exitCode != 0) {
            throw new RuntimeException("docker compose up 失败，退出码: " + exitCode);
        }

        // 等待集群就绪
        waitForReady();

        // 写入配置文件
        writeClusterConfig();

        log.info("========== RocketMQ 测试集群启动完成 ==========");
        return true;
    }

    /**
     * 停止测试集群
     * <p>
     * 执行 docker compose down -v，清理容器和卷。
     */
    public void stopCluster() {
        log.info("========== 停止 RocketMQ 测试集群 ==========");
        try {
            execCommand(projectRoot, "docker", "compose", "-f", COMPOSE_FILE, "down", "-v");
            log.info("测试集群已停止");
        } catch (Exception e) {
            log.warn("停止集群时出错（可能集群未运行）: {}", e.getMessage());
        }

        // 清理配置文件
        File configFile = new File(projectRoot, CONFIG_FILE);
        if (configFile.exists()) {
            configFile.delete();
            log.info("已清理集群配置文件: {}", configFile.getAbsolutePath());
        }
    }

    /**
     * 等待集群完全就绪
     * <p>
     * 轮询 NameServer 和 Broker 端口直到所有服务可连接。
     *
     * @throws Exception 超时未就绪
     */
    public void waitForReady() throws Exception {
        log.info("等待集群就绪（超时 {}s）...", READY_TIMEOUT_SECONDS);

        int[][] checkpoints = {
                {9876, 0},  // NameServer-1
                {9877, 0},  // NameServer-2
                {9878, 0},  // NameServer-3
                {10911, 0}, // src-broker-a
                {10921, 0}, // src-broker-b
                {10931, 0}, // tgt-broker-a
                {10941, 0}, // tgt-broker-b
        };
        String[] names = {
                "NameServer-1", "NameServer-2", "NameServer-3",
                "src-broker-a", "src-broker-b",
                "tgt-broker-a", "tgt-broker-b"
        };

        long deadline = System.currentTimeMillis() + READY_TIMEOUT_SECONDS * 1000L;

        while (System.currentTimeMillis() < deadline) {
            boolean allReady = true;
            for (int i = 0; i < checkpoints.length; i++) {
                if (checkpoints[i][1] == 0) {
                    if (isPortReachable("127.0.0.1", checkpoints[i][0], 2000)) {
                        checkpoints[i][1] = 1;
                        log.info("  ✓ {} (:{}) 就绪", names[i], checkpoints[i][0]);
                    } else {
                        allReady = false;
                    }
                }
            }

            if (allReady) {
                log.info("全部 {} 个服务已就绪", checkpoints.length);
                // 额外等待 5 秒让 Broker 完成注册
                Thread.sleep(5000);
                return;
            }

            Thread.sleep(POLL_INTERVAL_SECONDS * 1000L);
        }

        // 超时，打印未就绪的服务
        StringBuilder sb = new StringBuilder("以下服务未就绪: ");
        for (int i = 0; i < checkpoints.length; i++) {
            if (checkpoints[i][1] == 0) {
                sb.append(names[i]).append("(:").append(checkpoints[i][0]).append(") ");
            }
        }
        throw new RuntimeException("集群启动超时（" + READY_TIMEOUT_SECONDS + "s）— " + sb.toString());
    }

    /**
     * 将集群信息写入本地配置文件
     * <p>
     * 文件路径：target/test-cluster.properties
     */
    public void writeClusterConfig() throws IOException {
        Properties props = new Properties();
        props.setProperty("source.namesrv.addr", NAMESRV_ADDR);
        props.setProperty("target.namesrv.addr", NAMESRV_ADDR);
        props.setProperty("source.cluster.name", SOURCE_CLUSTER_NAME);
        props.setProperty("target.cluster.name", SINK_CLUSTER_NAME);
        props.setProperty("dashboard.url", DASHBOARD_URL);
        props.setProperty("src.broker.a.port", "10911");
        props.setProperty("src.broker.b.port", "10921");
        props.setProperty("tgt.broker.a.port", "10931");
        props.setProperty("tgt.broker.b.port", "10941");
        props.setProperty("cluster.ready", "true");
        props.setProperty("cluster.start.time", String.valueOf(System.currentTimeMillis()));

        Path configPath = Paths.get(projectRoot, CONFIG_FILE);
        Files.createDirectories(configPath.getParent());

        try (OutputStream os = new FileOutputStream(configPath.toFile())) {
            props.store(os, "RocketMQ Test Cluster Configuration (auto-generated)");
        }

        log.info("集群配置已写入: {}", configPath.toAbsolutePath());
    }

    /**
     * 从本地文件读取集群配置
     *
     * @return 集群配置属性，null 表示文件不存在
     */
    public static Properties loadClusterConfig() {
        return loadClusterConfig(null);
    }

    /**
     * 从本地文件读取集群配置
     *
     * @param projectRoot 项目根目录，null 时自动探测
     * @return 集群配置属性，null 表示文件不存在
     */
    public static Properties loadClusterConfig(String projectRoot) {
        String root = projectRoot != null ? projectRoot : detectProjectRoot();
        Path configPath = Paths.get(root, CONFIG_FILE);

        if (!Files.exists(configPath)) {
            return null;
        }

        Properties props = new Properties();
        try (InputStream is = new FileInputStream(configPath.toFile())) {
            props.load(is);
            return props;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 检查集群是否正在运行
     */
    public boolean isClusterRunning() {
        return isPortReachable("127.0.0.1", 9876, 2000)
                && isPortReachable("127.0.0.1", 10911, 2000)
                && isPortReachable("127.0.0.1", 10931, 2000);
    }

    /**
     * 检查端口是否可达
     */
    private static boolean isPortReachable(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 执行系统命令
     */
    private int execCommand(String workDir, String... command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(new File(workDir));
        pb.redirectErrorStream(true);
        pb.inheritIO();

        Process process = pb.start();
        boolean finished = process.waitFor(300, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new RuntimeException("命令执行超时: " + String.join(" ", command));
        }
        return process.exitValue();
    }

    /**
     * 自动探测项目根目录
     */
    private static String detectProjectRoot() {
        // 尝试从当前工作目录开始向上查找 pom.xml
        File dir = new File(System.getProperty("user.dir"));
        while (dir != null) {
            if (new File(dir, "pom.xml").exists() && new File(dir, "docker-compose.yml").exists()) {
                return dir.getAbsolutePath();
            }
            // 也检查 target 目录所在的父目录
            if (new File(dir, "pom.xml").exists()) {
                return dir.getAbsolutePath();
            }
            dir = dir.getParentFile();
        }
        // 兜底使用当前目录
        return System.getProperty("user.dir");
    }
}
