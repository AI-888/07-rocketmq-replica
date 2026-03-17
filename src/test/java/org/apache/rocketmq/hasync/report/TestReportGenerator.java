package org.apache.rocketmq.hasync.report;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试报告生成器 — 生成 HTML + JSON 格式的详细测试报告
 * <p>
 * 使用方式：
 * <pre>
 *   TestReportGenerator report = new TestReportGenerator("E2E测试报告");
 *   report.recordTestResult("场景分类", "testName", true, 120, null);
 *   report.recordTestResult("场景分类", "testFail", false, 50, "AssertionError: ...");
 *   report.generateReport("target/test-reports");
 * </pre>
 */
public class TestReportGenerator {

    private static final Logger log = LoggerFactory.getLogger(TestReportGenerator.class);

    private final String reportTitle;
    private final long startTime;
    private final Map<String, List<TestCaseResult>> categoryResults = new ConcurrentHashMap<>();
    private final AtomicInteger totalPassed = new AtomicInteger(0);
    private final AtomicInteger totalFailed = new AtomicInteger(0);
    private final AtomicInteger totalSkipped = new AtomicInteger(0);

    public TestReportGenerator(String reportTitle) {
        this.reportTitle = reportTitle;
        this.startTime = System.currentTimeMillis();
    }

    /**
     * 记录单个测试用例结果
     *
     * @param category  场景分类（如"数据流测试"、"异常恢复测试"）
     * @param testName  测试方法名
     * @param passed    是否通过
     * @param durationMs 耗时（毫秒）
     * @param errorMsg  失败信息（通过时为 null）
     */
    public void recordTestResult(String category, String testName, boolean passed,
                                  long durationMs, String errorMsg) {
        TestCaseResult result = new TestCaseResult(testName, passed, durationMs, errorMsg);
        categoryResults.computeIfAbsent(category, k -> new CopyOnWriteArrayList<>()).add(result);
        if (passed) {
            totalPassed.incrementAndGet();
        } else {
            totalFailed.incrementAndGet();
        }
    }

    /**
     * 记录跳过的测试
     */
    public void recordSkipped(String category, String testName, String reason) {
        TestCaseResult result = new TestCaseResult(testName, false, 0, "SKIPPED: " + reason);
        result.skipped = true;
        categoryResults.computeIfAbsent(category, k -> new CopyOnWriteArrayList<>()).add(result);
        totalSkipped.incrementAndGet();
    }

    /**
     * 生成完整测试报告（HTML + JSON + 控制台摘要）
     *
     * @param outputDir 输出目录
     */
    public void generateReport(String outputDir) throws IOException {
        long totalDuration = System.currentTimeMillis() - startTime;
        Path outPath = Paths.get(outputDir);
        Files.createDirectories(outPath);

        // 1. 生成 JSON 报告
        generateJsonReport(outPath, totalDuration);

        // 2. 生成 HTML 报告
        generateHtmlReport(outPath, totalDuration);

        // 3. 控制台摘要
        printConsoleSummary(totalDuration);

        log.info("测试报告已生成: {}", outPath.toAbsolutePath());
    }

    // ==================== JSON 报告 ====================

    private void generateJsonReport(Path outPath, long totalDuration) throws IOException {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("title", reportTitle);
        report.put("generatedAt", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        report.put("totalDurationMs", totalDuration);

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("total", totalPassed.get() + totalFailed.get() + totalSkipped.get());
        summary.put("passed", totalPassed.get());
        summary.put("failed", totalFailed.get());
        summary.put("skipped", totalSkipped.get());
        summary.put("passRate", calcPassRate());
        report.put("summary", summary);

        Map<String, Object> categories = new LinkedHashMap<>();
        for (Map.Entry<String, List<TestCaseResult>> entry : categoryResults.entrySet()) {
            Map<String, Object> catInfo = new LinkedHashMap<>();
            List<TestCaseResult> results = entry.getValue();
            long catPassed = results.stream().filter(r -> r.passed).count();
            long catFailed = results.stream().filter(r -> !r.passed && !r.skipped).count();
            long catSkipped = results.stream().filter(r -> r.skipped).count();

            catInfo.put("total", results.size());
            catInfo.put("passed", catPassed);
            catInfo.put("failed", catFailed);
            catInfo.put("skipped", catSkipped);

            List<Map<String, Object>> cases = new ArrayList<>();
            for (TestCaseResult r : results) {
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("name", r.testName);
                c.put("status", r.skipped ? "SKIPPED" : (r.passed ? "PASSED" : "FAILED"));
                c.put("durationMs", r.durationMs);
                if (r.errorMsg != null) c.put("error", r.errorMsg);
                cases.add(c);
            }
            catInfo.put("cases", cases);
            categories.put(entry.getKey(), catInfo);
        }
        report.put("categories", categories);

        Path jsonFile = outPath.resolve("report.json");
        try (Writer w = new OutputStreamWriter(new FileOutputStream(jsonFile.toFile()), StandardCharsets.UTF_8)) {
            w.write(JSON.toJSONString(report, JSONWriter.Feature.PrettyFormat));
        }
    }

    // ==================== HTML 报告 ====================

    private void generateHtmlReport(Path outPath, long totalDuration) throws IOException {
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n<html lang=\"zh-CN\">\n<head>\n");
        html.append("<meta charset=\"UTF-8\">\n");
        html.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
        html.append("<title>").append(reportTitle).append("</title>\n");
        html.append("<style>\n");
        html.append(getStyleSheet());
        html.append("</style>\n</head>\n<body>\n");

        // 头部
        html.append("<div class=\"header\">\n");
        html.append("  <h1>").append(reportTitle).append("</h1>\n");
        html.append("  <p class=\"meta\">生成时间: ")
                .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
                .append(" | 总耗时: ").append(formatDuration(totalDuration)).append("</p>\n");
        html.append("</div>\n");

        // 概览卡片
        int total = totalPassed.get() + totalFailed.get() + totalSkipped.get();
        html.append("<div class=\"summary-cards\">\n");
        html.append(summaryCard("总用例", String.valueOf(total), "card-total"));
        html.append(summaryCard("通过", String.valueOf(totalPassed.get()), "card-pass"));
        html.append(summaryCard("失败", String.valueOf(totalFailed.get()), "card-fail"));
        html.append(summaryCard("跳过", String.valueOf(totalSkipped.get()), "card-skip"));
        html.append(summaryCard("通过率", calcPassRate(), "card-rate"));
        html.append("</div>\n");

        // 进度条
        double passRatio = total > 0 ? (double) totalPassed.get() / total * 100 : 0;
        html.append("<div class=\"progress-bar\">\n");
        html.append("  <div class=\"progress-fill\" style=\"width:").append(String.format("%.1f", passRatio)).append("%\"></div>\n");
        html.append("</div>\n");

        // 分类详情
        for (Map.Entry<String, List<TestCaseResult>> entry : categoryResults.entrySet()) {
            String category = entry.getKey();
            List<TestCaseResult> results = entry.getValue();
            long catPassed = results.stream().filter(r -> r.passed).count();
            long catFailed = results.stream().filter(r -> !r.passed && !r.skipped).count();
            long catSkipped = results.stream().filter(r -> r.skipped).count();

            html.append("<div class=\"category\">\n");
            html.append("  <h2>").append(category);
            html.append("  <span class=\"badge badge-total\">").append(results.size()).append("</span>");
            html.append("  <span class=\"badge badge-pass\">✓ ").append(catPassed).append("</span>");
            if (catFailed > 0)
                html.append("  <span class=\"badge badge-fail\">✗ ").append(catFailed).append("</span>");
            if (catSkipped > 0)
                html.append("  <span class=\"badge badge-skip\">⊘ ").append(catSkipped).append("</span>");
            html.append("</h2>\n");

            html.append("  <table>\n");
            html.append("    <thead><tr><th>测试用例</th><th>状态</th><th>耗时</th><th>错误信息</th></tr></thead>\n");
            html.append("    <tbody>\n");
            for (TestCaseResult r : results) {
                String statusClass = r.skipped ? "status-skip" : (r.passed ? "status-pass" : "status-fail");
                String statusText = r.skipped ? "⊘ SKIPPED" : (r.passed ? "✓ PASSED" : "✗ FAILED");
                html.append("    <tr class=\"").append(statusClass).append("\">");
                html.append("<td>").append(escapeHtml(r.testName)).append("</td>");
                html.append("<td><span class=\"status ").append(statusClass).append("\">").append(statusText).append("</span></td>");
                html.append("<td>").append(r.durationMs).append("ms</td>");
                html.append("<td class=\"error-msg\">").append(r.errorMsg != null ? escapeHtml(r.errorMsg) : "-").append("</td>");
                html.append("</tr>\n");
            }
            html.append("    </tbody>\n  </table>\n</div>\n");
        }

        html.append("<div class=\"footer\">RocketMQ HA Sync — 自动化测试报告</div>\n");
        html.append("</body>\n</html>");

        Path htmlFile = outPath.resolve("index.html");
        try (Writer w = new OutputStreamWriter(new FileOutputStream(htmlFile.toFile()), StandardCharsets.UTF_8)) {
            w.write(html.toString());
        }
    }

    // ==================== 控制台摘要 ====================

    private void printConsoleSummary(long totalDuration) {
        int total = totalPassed.get() + totalFailed.get() + totalSkipped.get();

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║                    " + reportTitle + "                    ║");
        System.out.println("╠══════════════════════════════════════════════════════════╣");
        System.out.printf("║  总用例: %-6d  通过: %-6d  失败: %-6d  跳过: %-4d  ║%n",
                total, totalPassed.get(), totalFailed.get(), totalSkipped.get());
        System.out.printf("║  通过率: %-8s  总耗时: %-30s  ║%n", calcPassRate(), formatDuration(totalDuration));
        System.out.println("╠══════════════════════════════════════════════════════════╣");

        for (Map.Entry<String, List<TestCaseResult>> entry : categoryResults.entrySet()) {
            List<TestCaseResult> results = entry.getValue();
            long p = results.stream().filter(r -> r.passed).count();
            long f = results.stream().filter(r -> !r.passed && !r.skipped).count();
            System.out.printf("║  %-30s  %d/%d (失败: %d)  ║%n",
                    entry.getKey(), p, results.size(), f);
        }

        System.out.println("╚══════════════════════════════════════════════════════════╝");

        // 打印失败用例详情
        if (totalFailed.get() > 0) {
            System.out.println("\n--- 失败用例详情 ---");
            for (Map.Entry<String, List<TestCaseResult>> entry : categoryResults.entrySet()) {
                for (TestCaseResult r : entry.getValue()) {
                    if (!r.passed && !r.skipped) {
                        System.out.printf("  [FAIL] %s > %s%n", entry.getKey(), r.testName);
                        if (r.errorMsg != null) {
                            System.out.printf("         %s%n", r.errorMsg);
                        }
                    }
                }
            }
        }
        System.out.println();
    }

    // ==================== 辅助方法 ====================

    private String calcPassRate() {
        int total = totalPassed.get() + totalFailed.get();
        if (total == 0) return "N/A";
        return String.format("%.1f%%", (double) totalPassed.get() / total * 100);
    }

    private String formatDuration(long ms) {
        if (ms < 1000) return ms + "ms";
        if (ms < 60_000) return String.format("%.1fs", ms / 1000.0);
        return String.format("%dm %ds", ms / 60_000, (ms % 60_000) / 1000);
    }

    private String summaryCard(String label, String value, String cssClass) {
        return "  <div class=\"card " + cssClass + "\"><div class=\"card-value\">" +
                value + "</div><div class=\"card-label\">" + label + "</div></div>\n";
    }

    private String escapeHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("\n", "<br>");
    }

    private String getStyleSheet() {
        return "* { margin: 0; padding: 0; box-sizing: border-box; }\n" +
                "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f0f2f5; color: #333; padding: 24px; }\n" +
                ".header { text-align: center; margin-bottom: 32px; }\n" +
                ".header h1 { font-size: 28px; color: #1a1a2e; margin-bottom: 8px; }\n" +
                ".meta { color: #666; font-size: 14px; }\n" +
                ".summary-cards { display: flex; gap: 16px; justify-content: center; flex-wrap: wrap; margin-bottom: 24px; }\n" +
                ".card { background: #fff; border-radius: 12px; padding: 20px 32px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.08); min-width: 120px; transition: transform 0.2s; }\n" +
                ".card:hover { transform: translateY(-2px); box-shadow: 0 4px 16px rgba(0,0,0,0.12); }\n" +
                ".card-value { font-size: 36px; font-weight: 700; }\n" +
                ".card-label { font-size: 13px; color: #888; margin-top: 4px; }\n" +
                ".card-total .card-value { color: #1a73e8; }\n" +
                ".card-pass .card-value { color: #34a853; }\n" +
                ".card-fail .card-value { color: #ea4335; }\n" +
                ".card-skip .card-value { color: #fbbc04; }\n" +
                ".card-rate .card-value { color: #673ab7; }\n" +
                ".progress-bar { background: #e0e0e0; border-radius: 8px; height: 12px; margin-bottom: 32px; overflow: hidden; }\n" +
                ".progress-fill { background: linear-gradient(90deg, #34a853, #4caf50); height: 100%; border-radius: 8px; transition: width 0.5s ease; }\n" +
                ".category { background: #fff; border-radius: 12px; margin-bottom: 24px; padding: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }\n" +
                ".category h2 { font-size: 18px; margin-bottom: 16px; color: #1a1a2e; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }\n" +
                ".badge { font-size: 12px; padding: 2px 10px; border-radius: 12px; font-weight: 600; }\n" +
                ".badge-total { background: #e3f2fd; color: #1565c0; }\n" +
                ".badge-pass { background: #e8f5e9; color: #2e7d32; }\n" +
                ".badge-fail { background: #fce4ec; color: #c62828; }\n" +
                ".badge-skip { background: #fff8e1; color: #f57f17; }\n" +
                "table { width: 100%; border-collapse: collapse; font-size: 14px; }\n" +
                "th { background: #f5f5f5; padding: 10px 12px; text-align: left; border-bottom: 2px solid #e0e0e0; font-weight: 600; }\n" +
                "td { padding: 10px 12px; border-bottom: 1px solid #f0f0f0; }\n" +
                "tr:hover { background: #fafafa; }\n" +
                ".status { padding: 3px 10px; border-radius: 4px; font-size: 12px; font-weight: 600; }\n" +
                ".status-pass .status { background: #e8f5e9; color: #2e7d32; }\n" +
                ".status-fail .status { background: #fce4ec; color: #c62828; }\n" +
                ".status-skip .status { background: #fff8e1; color: #f57f17; }\n" +
                ".error-msg { font-size: 12px; color: #999; max-width: 400px; word-break: break-all; }\n" +
                ".footer { text-align: center; color: #999; font-size: 12px; margin-top: 32px; padding: 16px; }\n";
    }

    // ==================== 内部数据类 ====================

    public static class TestCaseResult {
        public final String testName;
        public final boolean passed;
        public final long durationMs;
        public final String errorMsg;
        public boolean skipped = false;

        public TestCaseResult(String testName, boolean passed, long durationMs, String errorMsg) {
            this.testName = testName;
            this.passed = passed;
            this.durationMs = durationMs;
            this.errorMsg = errorMsg;
        }
    }

    // ==================== Getters ====================

    public int getTotalPassed() { return totalPassed.get(); }
    public int getTotalFailed() { return totalFailed.get(); }
    public int getTotalSkipped() { return totalSkipped.get(); }
    public int getTotal() { return totalPassed.get() + totalFailed.get() + totalSkipped.get(); }
    public Map<String, List<TestCaseResult>> getCategoryResults() { return categoryResults; }
}
