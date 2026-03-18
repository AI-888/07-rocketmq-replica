package org.apache.rocketmq.hasync;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NumberedTestListener - JUnit4 RunListener
 *
 * 功能：
 * 1. 执行测试时打印编号和描述
 * 2. 检测并醒目输出关键阶段：资源创建、集群操作、位点管理、生命周期、资源清理等
 * 3. 按测试类分组统计，生成汇总报告
 */
public class NumberedTestListener extends RunListener {

    private final AtomicInteger counter = new AtomicInteger(0);
    private String currentClassName = null;
    private long testStartNano;

    private int passCount = 0;
    private int failCount = 0;
    private int skipCount = 0;

    private boolean currentTestFailed = false;

    /** className -> [pass, fail, skip] */
    private final Map<String, int[]> classStats = new LinkedHashMap<>();

    // ANSI 颜色
    private static final String RESET   = "\033[0m";
    private static final String BOLD    = "\033[1m";
    private static final String GREEN   = "\033[32m";
    private static final String RED     = "\033[31m";
    private static final String YELLOW  = "\033[33m";
    private static final String CYAN    = "\033[36m";
    private static final String DIM     = "\033[2m";
    private static final String MAGENTA = "\033[35m";
    private static final String BLUE    = "\033[34m";
    private static final String WHITE   = "\033[37m";
    private static final String BG_BLUE    = "\033[44m";
    private static final String BG_GREEN   = "\033[42m";
    private static final String BG_RED     = "\033[41m";
    private static final String BG_YELLOW  = "\033[43m";
    private static final String BG_MAGENTA = "\033[45m";
    private static final String BG_CYAN    = "\033[46m";

    // ---- 关键操作关键字 ----

    /** 资源创建类关键字 */
    private static final String[] CREATE_KEYWORDS = {
        "Create", "Init", "Setup", "Build", "Construct", "Alloc", "Register",
        "StartCluster", "StartPipeline", "Bootstrap", "Open", "Connect"
    };

    /** 资源清理类关键字 */
    private static final String[] CLEANUP_KEYWORDS = {
        "Destroy", "Cleanup", "TearDown", "Shutdown", "Stop", "Close", "Release",
        "StopCluster", "StopPipeline", "Deregister", "Remove", "Delete", "Clear",
        "Dispose", "Terminate"
    };

    /** 集群操作类关键字 */
    private static final String[] CLUSTER_KEYWORDS = {
        "Cluster", "Broker", "NameServer", "NameSrv", "Docker", "Compose",
        "Connectivity", "HealthCheck", "Ready", "Replica", "Master", "Slave"
    };

    /** 位点/持久化类关键字 */
    private static final String[] CHECKPOINT_KEYWORDS = {
        "Checkpoint", "Commit", "Offset", "Persist", "Flush", "Snapshot",
        "Recovery", "Recover", "Consistency", "Restore"
    };

    /** 流水线/数据流类关键字 */
    private static final String[] PIPELINE_KEYWORDS = {
        "Pipeline", "DataFlow", "EndToEnd", "E2E", "Source", "Sink",
        "Queue", "Offer", "Poll", "Write", "Batch", "Sync"
    };

    /** 错误/故障处理类关键字 */
    private static final String[] FAULT_KEYWORDS = {
        "Error", "Exception", "Fail", "Retry", "Rollback", "Timeout",
        "Fault", "Recovery", "Graceful", "Corrupt", "Invalid"
    };

    // ---- 关键操作定义：类型 -> [emoji, 中文标签, 颜色] ----
    private static final String[][] KEY_OP_DEFS = {
        {"CLUSTER",    "\uD83C\uDFE2", "集群操作",   BG_BLUE},    // 🏢
        {"CHECKPOINT", "\uD83D\uDCCD", "位点管理",   BG_MAGENTA}, // 📍
        {"LIFECYCLE",  "\u267B\uFE0F", "生命周期",   BG_CYAN},    // ♻️
        {"CLEANUP",    "\uD83E\uDDF9", "资源清理",   BG_RED},     // 🧹
        {"CREATE",     "\uD83D\uDEE0", "资源创建",   BG_GREEN},   // 🛠
        {"PIPELINE",   "\uD83D\uDD17", "流水线操作", BG_BLUE},    // 🔗
        {"FAULT",      "\u26A0\uFE0F", "故障处理",   BG_YELLOW},  // ⚠️
    };

    @Override
    public void testRunStarted(Description description) {
        System.out.println();
        System.out.println(CYAN + BOLD + "================================================================" + RESET);
        System.out.println(CYAN + BOLD + "  [#] TEST EXECUTION LIST (Numbered + Key Events)" + RESET);
        System.out.println(CYAN + BOLD + "================================================================" + RESET);
        System.out.println();
    }

    @Override
    public void testStarted(Description description) {
        String className = description.getTestClass().getSimpleName();

        if (!className.equals(currentClassName)) {
            if (currentClassName != null) {
                printClassSummary(currentClassName);
                System.out.println();
            }
            currentClassName = className;
            String divider = repeatChar('-', Math.max(1, 58 - className.length()));
            System.out.println(BOLD + "-- " + className + " " + divider + RESET);

            // 打印该测试类的生命周期信息
            printClassLifecycleInfo(description.getTestClass());
        }

        currentTestFailed = false;
        testStartNano = System.nanoTime();

        // 检测并输出关键操作阶段（醒目横幅）
        String methodName = description.getMethodName();
        printKeyOperationBanner(methodName);
    }

    @Override
    public void testFailure(Failure failure) {
        currentTestFailed = true;
    }

    @Override
    public void testIgnored(Description description) {
        String className = description.getTestClass().getSimpleName();

        if (!className.equals(currentClassName)) {
            if (currentClassName != null) {
                printClassSummary(currentClassName);
                System.out.println();
            }
            currentClassName = className;
            String divider = repeatChar('-', Math.max(1, 58 - className.length()));
            System.out.println(BOLD + "-- " + className + " " + divider + RESET);
            printClassLifecycleInfo(description.getTestClass());
        }

        int num = counter.incrementAndGet();
        skipCount++;

        String methodName = description.getMethodName();
        String desc = camelToDescription(methodName);

        String numStr = String.format("[%03d]", num);
        System.out.printf("  %s%s %sSKIP %-40s%s -- %s%n",
                YELLOW, numStr, YELLOW, methodName, RESET, DIM + desc + RESET);

        classStats.computeIfAbsent(className, k -> new int[3])[2]++;
    }

    @Override
    public void testFinished(Description description) {
        int num = counter.incrementAndGet();
        long elapsedMs = (System.nanoTime() - testStartNano) / 1_000_000;
        double elapsedSec = elapsedMs / 1000.0;

        String methodName = description.getMethodName();
        String className = description.getTestClass().getSimpleName();
        String desc = camelToDescription(methodName);

        String numStr = String.format("[%03d]", num);
        String timeStr = String.format("(%.3fs)", elapsedSec);

        // 构建内联标签
        String tags = buildTagsString(methodName);

        if (currentTestFailed) {
            failCount++;
            System.out.printf("  %s%s %sFAIL %-40s%s -- %-30s %s%s%n",
                    RED, numStr, RED, methodName, RESET, desc, DIM + timeStr + RESET, tags);
            classStats.computeIfAbsent(className, k -> new int[3])[1]++;
        } else {
            passCount++;
            System.out.printf("  %s%s %sPASS %-40s%s -- %-30s %s%s%n",
                    GREEN, numStr, GREEN, methodName, RESET, desc, DIM + timeStr + RESET, tags);
            classStats.computeIfAbsent(className, k -> new int[3])[0]++;
        }
    }

    @Override
    public void testRunFinished(Result result) {
        if (currentClassName != null) {
            printClassSummary(currentClassName);
        }

        int total = passCount + failCount + skipCount;

        System.out.println();
        System.out.println(CYAN + BOLD + "================================================================" + RESET);
        System.out.println(CYAN + BOLD + "  [*] TEST SUMMARY" + RESET);
        System.out.println(CYAN + BOLD + "================================================================" + RESET);
        System.out.println();
        System.out.printf("  Total:    %s%s%d%s tests%n", BOLD, CYAN, total, RESET);
        System.out.printf("  Passed:   %s%s%d%s%n", BOLD, GREEN, passCount, RESET);
        System.out.printf("  Failed:   %s%s%d%s%n", BOLD, RED, failCount, RESET);
        System.out.printf("  Skipped:  %s%s%d%s%n", BOLD, YELLOW, skipCount, RESET);
        System.out.printf("  Time:     %s%.3fs%s%n", BOLD, result.getRunTime() / 1000.0, RESET);
        System.out.println();

        // 每类统计表
        System.out.println(BOLD + "  Per-Class Statistics:" + RESET);
        System.out.printf("  %-45s %6s %6s %6s %6s%n", "Class Name", "Pass", "Fail", "Skip", "Total");
        System.out.println("  " + repeatChar('-', 75));

        for (Map.Entry<String, int[]> entry : classStats.entrySet()) {
            String name = entry.getKey();
            int[] stats = entry.getValue();
            int classTotal = stats[0] + stats[1] + stats[2];
            String statusIcon = stats[1] > 0 ? RED + "FAIL" + RESET : GREEN + " OK " + RESET;
            System.out.printf("  %s %-43s %s%6d%s %s%6d%s %s%6d%s %6d%n",
                    statusIcon, name,
                    GREEN, stats[0], RESET,
                    RED, stats[1], RESET,
                    YELLOW, stats[2], RESET,
                    classTotal);
        }
        System.out.println("  " + repeatChar('-', 75));
        System.out.println();

        if (failCount == 0) {
            System.out.println(GREEN + BOLD + "  >>> ALL " + total + " TESTS PASSED! <<<" + RESET);
        } else {
            System.out.println(RED + BOLD + "  >>> " + failCount + " TEST(S) FAILED! <<<" + RESET);
        }
        System.out.println();
    }

    // ============================================================
    //  关键操作检测 & 生命周期输出
    // ============================================================

    /**
     * 输出醒目的关键操作横幅
     */
    private void printKeyOperationBanner(String methodName) {
        List<String[]> matched = detectAllKeyOperations(methodName);
        if (matched.isEmpty()) {
            return;
        }

        for (String[] op : matched) {
            String type = op[0];
            String keyword = op[1];

            // 查找对应的 emoji / 标签 / 颜色
            String emoji = "\uD83D\uDD14"; // 🔔 默认
            String label = type;
            String bgColor = BG_BLUE;

            for (String[] def : KEY_OP_DEFS) {
                if (def[0].equals(type)) {
                    emoji = def[1];
                    label = def[2];
                    bgColor = def[3];
                    break;
                }
            }

            // 生成详细描述
            String detail = getKeyOperationDetail(type, keyword, methodName);

            // 输出醒目横幅
            System.out.printf("  %s%s %s %s %s %s%s  %s%n",
                    bgColor + WHITE + BOLD,
                    " " + emoji,
                    label,
                    "|",
                    keyword,
                    " ",
                    RESET,
                    DIM + detail + RESET);
        }
    }

    /**
     * 获取关键操作的详细中文描述
     */
    private String getKeyOperationDetail(String type, String keyword, String methodName) {
        switch (type) {
            case "CLUSTER":
                if (containsCamelWord(methodName, "Broker")) return "=> Broker 实例创建/连接";
                if (containsCamelWord(methodName, "NameServer") || containsCamelWord(methodName, "NameSrv"))
                    return "=> NameServer 服务操作";
                if (containsCamelWord(methodName, "Connectivity")) return "=> 集群连通性检查";
                if (containsCamelWord(methodName, "HealthCheck") || containsCamelWord(methodName, "Ready"))
                    return "=> 集群健康检查/就绪检查";
                if (containsCamelWord(methodName, "Master")) return "=> Master 节点操作";
                if (containsCamelWord(methodName, "Slave") || containsCamelWord(methodName, "Replica"))
                    return "=> Slave/副本节点操作";
                return "=> 集群环境操作";

            case "CHECKPOINT":
                if (containsCamelWord(methodName, "Snapshot")) return "=> 快照数据持久化/恢复";
                if (containsCamelWord(methodName, "Recovery") || containsCamelWord(methodName, "Recover"))
                    return "=> 位点故障恢复";
                if (containsCamelWord(methodName, "Consistency")) return "=> 启动一致性校验";
                if (containsCamelWord(methodName, "Persist") || containsCamelWord(methodName, "Flush"))
                    return "=> 位点数据刷盘";
                if (containsCamelWord(methodName, "Offset") || containsCamelWord(methodName, "Commit"))
                    return "=> 偏移量提交/读取";
                return "=> 位点数据操作";

            case "LIFECYCLE":
                return "=> 组件启动/停止生命周期";

            case "CLEANUP":
                if (containsCamelWord(methodName, "Shutdown")) return "=> 优雅停机/资源释放";
                if (containsCamelWord(methodName, "Close")) return "=> 连接/文件关闭";
                if (containsCamelWord(methodName, "Delete") || containsCamelWord(methodName, "Remove"))
                    return "=> 数据/资源删除";
                if (containsCamelWord(methodName, "Clear")) return "=> 缓存/队列清空";
                return "=> 资源销毁与清理";

            case "CREATE":
                if (containsCamelWord(methodName, "Bootstrap")) return "=> 应用引导初始化";
                if (containsCamelWord(methodName, "Register")) return "=> 组件/服务注册";
                if (containsCamelWord(methodName, "Connect") || containsCamelWord(methodName, "Open"))
                    return "=> 连接/通道建立";
                if (containsCamelWord(methodName, "Build") || containsCamelWord(methodName, "Construct"))
                    return "=> 对象构建与组装";
                return "=> 资源创建与初始化";

            case "PIPELINE":
                if (containsCamelWord(methodName, "EndToEnd") || containsCamelWord(methodName, "E2E"))
                    return "=> 端到端数据流验证";
                if (containsCamelWord(methodName, "Batch")) return "=> 批量数据处理";
                if (containsCamelWord(methodName, "Sync")) return "=> 数据同步操作";
                if (containsCamelWord(methodName, "Queue") || containsCamelWord(methodName, "Offer") ||
                    containsCamelWord(methodName, "Poll")) return "=> 队列读写操作";
                return "=> 数据流水线操作";

            case "FAULT":
                if (containsCamelWord(methodName, "Retry")) return "=> 失败重试机制验证";
                if (containsCamelWord(methodName, "Timeout")) return "=> 超时处理验证";
                if (containsCamelWord(methodName, "Graceful")) return "=> 优雅异常处理";
                if (containsCamelWord(methodName, "Corrupt")) return "=> 数据损坏场景";
                if (containsCamelWord(methodName, "Rollback")) return "=> 回滚机制验证";
                if (containsCamelWord(methodName, "Recovery")) return "=> 故障恢复验证";
                return "=> 异常/故障场景验证";

            default:
                return "";
        }
    }

    /**
     * 检测方法名中所有匹配的关键操作类型
     * @return List of [type, matchedKeyword]
     */
    private List<String[]> detectAllKeyOperations(String methodName) {
        List<String[]> result = new ArrayList<>();

        // 集群操作
        String kw = findMatchedKeyword(methodName, CLUSTER_KEYWORDS);
        if (kw != null) result.add(new String[]{"CLUSTER", kw});

        // 位点操作
        kw = findMatchedKeyword(methodName, CHECKPOINT_KEYWORDS);
        if (kw != null) result.add(new String[]{"CHECKPOINT", kw});

        // 生命周期（Start+Stop）
        if (containsStartStop(methodName)) {
            result.add(new String[]{"LIFECYCLE", "Start/Stop"});
        }

        // 资源清理
        kw = findMatchedKeyword(methodName, CLEANUP_KEYWORDS);
        if (kw != null) result.add(new String[]{"CLEANUP", kw});

        // 资源创建
        kw = findMatchedKeyword(methodName, CREATE_KEYWORDS);
        if (kw != null) result.add(new String[]{"CREATE", kw});

        // 流水线
        kw = findMatchedKeyword(methodName, PIPELINE_KEYWORDS);
        if (kw != null) result.add(new String[]{"PIPELINE", kw});

        // 故障处理
        kw = findMatchedKeyword(methodName, FAULT_KEYWORDS);
        if (kw != null) result.add(new String[]{"FAULT", kw});

        return result;
    }

    /**
     * 打印测试类的生命周期信息（setup/teardown 方法）
     */
    private void printClassLifecycleInfo(Class<?> testClass) {
        if (testClass == null) return;

        List<String> setupMethods = new ArrayList<>();
        List<String> teardownMethods = new ArrayList<>();

        try {
            for (Method method : testClass.getMethods()) {
                if (method.isAnnotationPresent(BeforeClass.class)) {
                    setupMethods.add(method.getName() + "()");
                }
                if (method.isAnnotationPresent(Before.class)) {
                    setupMethods.add(method.getName() + "()");
                }
                if (method.isAnnotationPresent(AfterClass.class)) {
                    teardownMethods.add(method.getName() + "()");
                }
                if (method.isAnnotationPresent(After.class)) {
                    teardownMethods.add(method.getName() + "()");
                }
            }
        } catch (Exception e) {
            // 忽略反射错误
        }

        // 输出资源生命周期摘要
        if (!setupMethods.isEmpty() || !teardownMethods.isEmpty()) {
            System.out.printf("  %s|-- \u267B\uFE0F  Lifecycle:%s%n", BLUE + BOLD, RESET);
            for (String s : setupMethods) {
                System.out.printf("  %s|   \u2795  SETUP   %s%s%n", BLUE, s, RESET);
            }
            for (String t : teardownMethods) {
                System.out.printf("  %s|   \u2796  CLEANUP %s%s%n", BLUE, t, RESET);
            }
        }

        // 检测该类使用/管理的资源
        List<String> resources = detectClassResources(testClass);
        if (!resources.isEmpty()) {
            System.out.printf("  %s|-- \uD83D\uDCE6  Resources: %s%s%n", MAGENTA, String.join(", ", resources), RESET);
        }
    }

    /**
     * 通过字段类型扫描检测测试类使用的资源/组件
     */
    private List<String> detectClassResources(Class<?> testClass) {
        List<String> resources = new ArrayList<>();
        try {
            java.lang.reflect.Field[] fields = testClass.getDeclaredFields();
            for (java.lang.reflect.Field field : fields) {
                String typeName = field.getType().getSimpleName();
                String fieldName = field.getName();

                if (typeName.contains("Pipeline") || fieldName.contains("pipeline")) {
                    addUnique(resources, "TestSyncPipelineHelper");
                } else if (typeName.contains("Source") || fieldName.contains("source")) {
                    addUnique(resources, "Source(" + typeName + ")");
                } else if (typeName.contains("Sink") || fieldName.contains("sink")) {
                    addUnique(resources, "Sink(" + typeName + ")");
                } else if (typeName.contains("Checkpoint") || fieldName.contains("checkpoint")) {
                    addUnique(resources, "Checkpoint(" + typeName + ")");
                } else if (typeName.contains("ClusterManager") || fieldName.contains("cluster")) {
                    addUnique(resources, "ClusterManager");
                } else if (typeName.contains("Report") || fieldName.contains("report")) {
                    addUnique(resources, "TestReport");
                } else if (typeName.contains("Metrics") || fieldName.contains("metrics")) {
                    addUnique(resources, "Metrics(" + typeName + ")");
                } else if (typeName.contains("Scheduler") || typeName.contains("Executor")) {
                    addUnique(resources, "Scheduler(" + typeName + ")");
                } else if (typeName.contains("HttpServer")) {
                    addUnique(resources, "HttpServer");
                } else if (typeName.contains("TraceCollector") || fieldName.contains("trace")) {
                    addUnique(resources, "TraceCollector");
                } else if (typeName.contains("AlertEvaluator") || fieldName.contains("alert")) {
                    addUnique(resources, "AlertEvaluator");
                }
            }
        } catch (Exception e) {
            // 忽略反射错误
        }
        return resources;
    }

    /**
     * 构建内联标签字符串，附加在测试结果行末尾
     */
    private String buildTagsString(String methodName) {
        List<String> tags = new ArrayList<>();

        if (containsAny(methodName, CLUSTER_KEYWORDS))    tags.add("CLUSTER");
        if (containsAny(methodName, CHECKPOINT_KEYWORDS))  tags.add("CKPT");
        if (containsAny(methodName, PIPELINE_KEYWORDS))    tags.add("PIPELINE");
        if (containsAny(methodName, CREATE_KEYWORDS))      tags.add("CREATE");
        if (containsAny(methodName, CLEANUP_KEYWORDS))     tags.add("CLEANUP");
        if (containsAny(methodName, FAULT_KEYWORDS))       tags.add("FAULT");

        if (tags.isEmpty()) return "";

        StringBuilder sb = new StringBuilder();
        sb.append(" ").append(CYAN).append("[");
        for (int i = 0; i < tags.size(); i++) {
            if (i > 0) sb.append("|");
            sb.append(tags.get(i));
        }
        sb.append("]").append(RESET);
        return sb.toString();
    }

    // ---- 辅助方法 ----

    private void printClassSummary(String className) {
        int[] stats = classStats.getOrDefault(className, new int[]{0, 0, 0});
        int classTotal = stats[0] + stats[1] + stats[2];
        String icon = stats[1] > 0 ? "FAIL" : "OK";
        System.out.printf("  %s-- [%s] subtotal: %d passed, %d failed, %d skipped (total %d)%s%n",
                DIM, icon, stats[0], stats[1], stats[2], classTotal, RESET);
    }

    /**
     * 驼峰命名转可读描述
     */
    private static String camelToDescription(String methodName) {
        String name = methodName;
        if (name.startsWith("test")) {
            name = name.substring(4);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (i > 0 && Character.isUpperCase(c)) {
                if (!Character.isUpperCase(name.charAt(i - 1))) {
                    sb.append(' ');
                }
            }
            sb.append(c);
        }
        return sb.toString().trim();
    }

    private static boolean containsAny(String text, String[] keywords) {
        for (String kw : keywords) {
            if (containsCamelWord(text, kw)) {
                return true;
            }
        }
        return false;
    }

    private static String findMatchedKeyword(String text, String[] keywords) {
        for (String kw : keywords) {
            if (containsCamelWord(text, kw)) {
                return kw;
            }
        }
        return null;
    }

    /**
     * 检查驼峰文本中是否包含关键字（作为独立词）
     */
    private static boolean containsCamelWord(String text, String keyword) {
        String lower = text.toLowerCase();
        String kwLower = keyword.toLowerCase();
        int idx = lower.indexOf(kwLower);
        while (idx >= 0) {
            boolean startOk = (idx == 0) || Character.isUpperCase(text.charAt(idx))
                    || !Character.isLetter(text.charAt(idx - 1));
            int endIdx = idx + kwLower.length();
            boolean endOk = (endIdx >= text.length()) || Character.isUpperCase(text.charAt(endIdx))
                    || !Character.isLetter(text.charAt(endIdx));
            if (startOk && endOk) {
                return true;
            }
            idx = lower.indexOf(kwLower, idx + 1);
        }
        return false;
    }

    private static boolean containsStartStop(String methodName) {
        String lower = methodName.toLowerCase();
        return (lower.contains("start") && lower.contains("stop"))
                || (lower.contains("start") && lower.contains("shutdown"))
                || lower.contains("startandstop");
    }

    private static String repeatChar(char c, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    private static void addUnique(List<String> list, String item) {
        if (!list.contains(item)) {
            list.add(item);
        }
    }
}
