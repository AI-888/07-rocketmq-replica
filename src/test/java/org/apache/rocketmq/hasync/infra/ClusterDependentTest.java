package org.apache.rocketmq.hasync.infra;

import org.apache.rocketmq.hasync.report.TestReportGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * 集群依赖的单元测试
 * <p>
 * 测试前从本地文件读取集群信息。如果集群未部署或配置文件不存在，
 * 测试将以 SKIPPED 状态跳过（不失败）。
 * <p>
 * 覆盖场景：
 * <ul>
 *   <li>集群配置文件加载</li>
 *   <li>NameServer 连通性</li>
 *   <li>源集群 Broker 连通性</li>
 *   <li>目标集群 Broker 连通性</li>
 *   <li>Dashboard 连通性</li>
 *   <li>集群配置完整性校验</li>
 * </ul>
 */
public class ClusterDependentTest {

    private static TestReportGenerator report;
    private static Properties clusterConfig;
    private static boolean clusterAvailable = false;

    @BeforeClass
    public static void setUp() {
        report = new TestReportGenerator("单元测试报告（集群依赖）");

        // 尝试从本地文件加载集群配置
        clusterConfig = RocketMQClusterManager.loadClusterConfig();
        if (clusterConfig != null && "true".equals(clusterConfig.getProperty("cluster.ready"))) {
            // 快速检查 NameServer 是否可达
            clusterAvailable = isPortReachable("127.0.0.1", 9876, 2000);
        }

        if (!clusterAvailable) {
            System.out.println("[INFO] RocketMQ 测试集群不可用，集群依赖测试将以 SKIPPED 状态跳过");
        }
    }

    @AfterClass
    public static void generateReport() throws Exception {
        report.generateReport("target/test-reports/unit");
    }

    // ==================== 集群配置文件测试 ====================

    @Test
    public void testClusterConfigFileLoad() {
        long start = System.currentTimeMillis();
        try {
            if (!clusterAvailable) {
                report.recordSkipped("集群配置", "加载集群配置文件", "集群未部署");
                return; // 跳过，不失败
            }

            assertNotNull("集群配置应不为 null", clusterConfig);
            assertNotNull("source.namesrv.addr 应存在",
                    clusterConfig.getProperty("source.namesrv.addr"));
            assertNotNull("target.namesrv.addr 应存在",
                    clusterConfig.getProperty("target.namesrv.addr"));
            assertNotNull("source.cluster.name 应存在",
                    clusterConfig.getProperty("source.cluster.name"));
            assertNotNull("target.cluster.name 应存在",
                    clusterConfig.getProperty("target.cluster.name"));

            report.recordTestResult("集群配置", "加载集群配置文件", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("集群配置", "加载集群配置文件", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void testClusterConfigCompleteness() {
        long start = System.currentTimeMillis();
        try {
            if (!clusterAvailable) {
                report.recordSkipped("集群配置", "配置完整性校验", "集群未部署");
                return;
            }

            // 验证所有必须的配置项
            String[] requiredKeys = {
                    "source.namesrv.addr", "target.namesrv.addr",
                    "source.cluster.name", "target.cluster.name",
                    "dashboard.url",
                    "src.broker.a.port", "src.broker.b.port",
                    "tgt.broker.a.port", "tgt.broker.b.port",
                    "cluster.ready", "cluster.start.time"
            };

            for (String key : requiredKeys) {
                assertNotNull("缺少配置项: " + key, clusterConfig.getProperty(key));
                assertFalse("配置项为空: " + key, clusterConfig.getProperty(key).isEmpty());
            }

            // 验证集群名称不同
            assertNotEquals("源集群和目标集群名称应不同",
                    clusterConfig.getProperty("source.cluster.name"),
                    clusterConfig.getProperty("target.cluster.name"));

            report.recordTestResult("集群配置", "配置完整性校验", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("集群配置", "配置完整性校验", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== NameServer 连通性测试 ====================

    @Test
    public void testNameServer1Connectivity() {
        testPortConnectivity("NameServer连通性", "NameServer-1 (:9876)", 9876);
    }

    @Test
    public void testNameServer2Connectivity() {
        testPortConnectivity("NameServer连通性", "NameServer-2 (:9877)", 9877);
    }

    @Test
    public void testNameServer3Connectivity() {
        testPortConnectivity("NameServer连通性", "NameServer-3 (:9878)", 9878);
    }

    // ==================== 源集群 Broker 连通性测试 ====================

    @Test
    public void testSrcBrokerAConnectivity() {
        testPortConnectivity("源集群Broker连通性", "src-broker-a (:10911)", 10911);
    }

    @Test
    public void testSrcBrokerBConnectivity() {
        testPortConnectivity("源集群Broker连通性", "src-broker-b (:10921)", 10921);
    }

    // ==================== 目标集群 Broker 连通性测试 ====================

    @Test
    public void testTgtBrokerAConnectivity() {
        testPortConnectivity("目标集群Broker连通性", "tgt-broker-a (:10931)", 10931);
    }

    @Test
    public void testTgtBrokerBConnectivity() {
        testPortConnectivity("目标集群Broker连通性", "tgt-broker-b (:10941)", 10941);
    }

    // ==================== Dashboard 连通性测试 ====================

    @Test
    public void testDashboardConnectivity() {
        testPortConnectivity("Dashboard连通性", "Dashboard (:8080)", 8080);
    }

    // ==================== 集群管理器测试 ====================

    @Test
    public void testClusterManagerDetectRunning() {
        long start = System.currentTimeMillis();
        try {
            RocketMQClusterManager manager = new RocketMQClusterManager();
            boolean running = manager.isClusterRunning();

            if (clusterAvailable) {
                assertTrue("集群已部署时 isClusterRunning 应返回 true", running);
            }
            // 无论集群是否可用，方法本身不应抛异常

            report.recordTestResult("集群管理器", "isClusterRunning检测", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("集群管理器", "isClusterRunning检测", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void testClusterManagerConfigConstants() {
        long start = System.currentTimeMillis();
        try {
            assertEquals("SourceCluster", RocketMQClusterManager.SOURCE_CLUSTER_NAME);
            assertEquals("SinkCluster", RocketMQClusterManager.SINK_CLUSTER_NAME);
            assertNotNull(RocketMQClusterManager.NAMESRV_ADDR);
            assertTrue(RocketMQClusterManager.NAMESRV_ADDR.contains("9876"));
            assertNotNull(RocketMQClusterManager.DASHBOARD_URL);
            assertTrue(RocketMQClusterManager.DASHBOARD_URL.contains("8080"));

            report.recordTestResult("集群管理器", "配置常量正确性", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("集群管理器", "配置常量正确性", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void testLoadClusterConfigWithNullRoot() {
        long start = System.currentTimeMillis();
        try {
            // loadClusterConfig(null) 应不抛异常
            Properties props = RocketMQClusterManager.loadClusterConfig(null);
            // 返回值取决于文件是否存在，但不应抛异常

            report.recordTestResult("集群管理器", "loadClusterConfig(null)不抛异常", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("集群管理器", "loadClusterConfig(null)不抛异常", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void testLoadClusterConfigWithInvalidPath() {
        long start = System.currentTimeMillis();
        try {
            Properties props = RocketMQClusterManager.loadClusterConfig("/non/existent/path");
            assertNull("不存在的路径应返回 null", props);

            report.recordTestResult("集群管理器", "loadClusterConfig(无效路径)返回null", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("集群管理器", "loadClusterConfig(无效路径)返回null", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 测试报告生成器自测 ====================

    @Test
    public void testReportGeneratorSelf() {
        long start = System.currentTimeMillis();
        try {
            TestReportGenerator selfReport = new TestReportGenerator("自测报告");
            selfReport.recordTestResult("分类A", "测试1", true, 10, null);
            selfReport.recordTestResult("分类A", "测试2", false, 20, "断言失败");
            selfReport.recordSkipped("分类B", "测试3", "条件不满足");

            assertEquals(1, selfReport.getTotalPassed());
            assertEquals(1, selfReport.getTotalFailed());
            assertEquals(1, selfReport.getTotalSkipped());
            assertEquals(3, selfReport.getTotal());
            assertEquals(2, selfReport.getCategoryResults().size());

            // 生成报告到临时目录
            selfReport.generateReport("target/test-reports/self-test");

            // 验证报告文件已生成
            assertTrue("HTML 报告应存在", new File("target/test-reports/self-test/index.html").exists());
            assertTrue("JSON 报告应存在", new File("target/test-reports/self-test/report.json").exists());

            report.recordTestResult("测试报告生成器", "报告生成器自测", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("测试报告生成器", "报告生成器自测", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void testReportGeneratorHtmlContent() {
        long start = System.currentTimeMillis();
        try {
            TestReportGenerator htmlReport = new TestReportGenerator("HTML内容测试");
            htmlReport.recordTestResult("测试类别", "成功用例", true, 5, null);
            htmlReport.recordTestResult("测试类别", "失败用例", false, 3, "Error msg");
            htmlReport.generateReport("target/test-reports/html-test");

            // 读取并验证 HTML 内容
            File htmlFile = new File("target/test-reports/html-test/index.html");
            assertTrue(htmlFile.exists());
            assertTrue(htmlFile.length() > 1000); // HTML 应有一定长度

            report.recordTestResult("测试报告生成器", "HTML报告内容完整性", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("测试报告生成器", "HTML报告内容完整性", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void testReportGeneratorJsonContent() {
        long start = System.currentTimeMillis();
        try {
            TestReportGenerator jsonReport = new TestReportGenerator("JSON内容测试");
            jsonReport.recordTestResult("数据流测试", "case1", true, 100, null);
            jsonReport.generateReport("target/test-reports/json-test");

            File jsonFile = new File("target/test-reports/json-test/report.json");
            assertTrue(jsonFile.exists());

            // 读取 JSON 内容
            byte[] bytes = new byte[(int) jsonFile.length()];
            try (FileInputStream fis = new FileInputStream(jsonFile)) {
                fis.read(bytes);
            }
            String json = new String(bytes, "UTF-8");
            assertTrue("JSON 应包含 title", json.contains("JSON内容测试"));
            assertTrue("JSON 应包含 summary", json.contains("summary"));
            assertTrue("JSON 应包含 categories", json.contains("categories"));
            assertTrue("JSON 应包含 passed", json.contains("passed"));

            report.recordTestResult("测试报告生成器", "JSON报告内容完整性", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("测试报告生成器", "JSON报告内容完整性", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 辅助方法 ====================

    private void testPortConnectivity(String category, String testName, int port) {
        long start = System.currentTimeMillis();
        try {
            if (!clusterAvailable) {
                report.recordSkipped(category, testName, "集群未部署");
                return;
            }

            assertTrue(testName + " 应可达", isPortReachable("127.0.0.1", port, 3000));

            report.recordTestResult(category, testName, true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult(category, testName, false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    private static boolean isPortReachable(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
