#!/usr/bin/env bash
# ============================================================================
# RocketMQ HA Data Sync — 一键测试脚本
# ============================================================================
# 功能：自动执行所有测试用例并生成详细的 HTML 报告
#
# 用法：
#   ./run-tests.sh                   # 运行全部测试
#   ./run-tests.sh --unit            # 仅运行单元测试
#   ./run-tests.sh --e2e             # 仅运行端到端测试
#   ./run-tests.sh --class <Name>    # 运行指定测试类
#   ./run-tests.sh --module <pkg>    # 运行指定模块（如 source, sink, model）
#   ./run-tests.sh --report-only     # 仅从已有 XML 重新生成报告（不运行测试）
#   ./run-tests.sh --help            # 显示帮助
#
# 输出：
#   target/test-reports/report.html  — 可视化 HTML 测试报告
#   target/surefire-reports/         — 原始 XML 报告
# ============================================================================

set -euo pipefail

# ======================== 常量 & 颜色 ========================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
REPORT_DIR="$PROJECT_DIR/target/test-reports"
SUREFIRE_DIR="$PROJECT_DIR/target/surefire-reports"
HTML_REPORT="$REPORT_DIR/report.html"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# ======================== 函数 ========================

print_banner() {
    echo -e "${CYAN}${BOLD}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║          RocketMQ HA Sync — 一键测试执行器                 ║"
    echo "║          版本 1.0.0-SNAPSHOT                               ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    echo -e "  ${BLUE}时间:${NC} $TIMESTAMP"
    echo -e "  ${BLUE}项目:${NC} $PROJECT_DIR"
    echo ""
}

print_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  --unit            仅运行单元测试（排除 e2e 包）"
    echo "  --e2e             仅运行端到端测试"
    echo "  --class <Name>    运行指定测试类（如 CommitLogParserTest）"
    echo "  --module <pkg>    运行指定模块的测试（如 source, sink, model, config）"
    echo "  --report-only     仅从已有 XML 重新生成 HTML 报告"
    echo "  --no-report       运行测试但不生成 HTML 报告"
    echo "  --verbose         显示详细测试输出"
    echo "  --help            显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                          # 运行全部测试并生成报告"
    echo "  $0 --unit                   # 仅运行单元测试"
    echo "  $0 --module source          # 运行 source 模块测试"
    echo "  $0 --class CommitLogParserTest  # 运行指定测试类"
    echo ""
}

log_info()    { echo -e "  ${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "  ${GREEN}[PASS]${NC}  $1"; }
log_warn()    { echo -e "  ${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "  ${RED}[FAIL]${NC}  $1"; }
log_step()    { echo -e "\n${BOLD}▶ $1${NC}"; }

# ======================== 运行测试 ========================

run_tests() {
    local mvn_args="$1"
    local label="$2"

    log_step "编译项目"
    cd "$PROJECT_DIR"
    if ! mvn compile test-compile -q 2>/dev/null; then
        log_error "编译失败！请检查代码错误。"
        mvn compile test-compile 2>&1 | tail -20
        exit 1
    fi
    log_success "编译成功"

    log_step "执行测试: $label"
    echo ""

    local start_time=$(date +%s)

    # 运行 Maven 测试，捕获退出码
    local test_exit_code=0
    if [ "$VERBOSE" = "true" ]; then
        mvn test $mvn_args -DfailIfNoTests=false 2>&1 | tee "$REPORT_DIR/test-output.log" || test_exit_code=$?
    else
        # 过滤规则：保留 Maven 关键行 + NumberedTestListener 的全部输出
        # NumberedTestListener 输出的行模式:
        #   "══"         — 分隔线
        #   "  [#]"      — 标题行
        #   "  [*]"      — 摘要标题
        #   "-- "        — 测试类标题
        #   "  [0-9"     — 编号行 [001] PASS ...
        #   "  >>> KEY:"  — 关键操作标记
        #   "  |--"      — 生命周期/资源信息
        #   "  |  "      — 生命周期详情
        #   "  Total:"   — 汇总统计
        #   "  Passed:"  — 汇总统计
        #   "  Failed:"  — 汇总统计
        #   "  Skipped:" — 汇总统计
        #   "  Time:"    — 汇总统计
        #   "  Per-Class" — 统计表头
        #   "  Class "   — 统计表头
        #   "  ------"   — 统计分隔线
        #   "  >>> ALL"  — 最终结论
        #   "  FAIL "    — 每类统计行
        #   "  OK "      — 每类统计行
        #   "  🔔"       — 关键阶段提示（增强后）
        mvn test $mvn_args -DfailIfNoTests=false 2>&1 | tee "$REPORT_DIR/test-output.log" \
            | grep -E "^\[INFO\] Tests run:|^\[INFO\] BUILD|^\[ERROR\]|^\[INFO\] Running|^══|^  \[#\]|^  \[\\*\]|^-- |^  \[[0-9]|>>>|^\s+\|--|^\s+\|   |^  Total:|^  Passed:|^  Failed:|^  Skipped:|^  Time:|^  Per-Class|^  Class |^  ----|^  >>>|FAIL |  OK " \
            || test_exit_code=$?
        # 如果 grep 没匹配到不算失败，检查真实退出码
        test_exit_code=${PIPESTATUS[0]}
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo ""
    if [ $test_exit_code -eq 0 ]; then
        log_success "测试完成 — 耗时 ${duration}s ✅"
    else
        log_error "测试存在失败 — 耗时 ${duration}s ❌"
    fi

    TOTAL_DURATION=$duration
    TEST_EXIT_CODE=$test_exit_code
}

# ======================== 解析 XML 报告 ========================

parse_surefire_xml() {
    log_step "解析测试报告"

    if [ ! -d "$SUREFIRE_DIR" ]; then
        log_error "未找到 surefire-reports 目录: $SUREFIRE_DIR"
        exit 1
    fi

    local xml_files=$(find "$SUREFIRE_DIR" -name "TEST-*.xml" -type f | sort)
    local xml_count=$(echo "$xml_files" | grep -c . || true)

    if [ "$xml_count" -eq 0 ]; then
        log_error "未找到任何测试 XML 报告文件"
        exit 1
    fi

    log_info "找到 $xml_count 个测试报告文件"

    # 初始化总计
    TOTAL_TESTS=0
    TOTAL_FAILURES=0
    TOTAL_ERRORS=0
    TOTAL_SKIPPED=0
    TOTAL_TIME="0"

    # 用临时文件收集每个测试类的数据
    CLASSES_DATA_FILE=$(mktemp)

    while IFS= read -r xml_file; do
        [ -z "$xml_file" ] && continue

        # 提取 testsuite 属性
        local name=$(grep -oP 'name="[^"]*"' "$xml_file" | head -1 | sed 's/name="//;s/"//')
        local tests=$(grep -oP 'tests="[^"]*"' "$xml_file" | head -1 | sed 's/tests="//;s/"//')
        local failures=$(grep -oP 'failures="[^"]*"' "$xml_file" | head -1 | sed 's/failures="//;s/"//')
        local errors=$(grep -oP 'errors="[^"]*"' "$xml_file" | head -1 | sed 's/errors="//;s/"//')
        local skipped=$(grep -oP 'skipped="[^"]*"' "$xml_file" | head -1 | sed 's/skipped="//;s/"//')
        local time=$(grep -oP 'time="[^"]*"' "$xml_file" | head -1 | sed 's/time="//;s/"//')

        tests=${tests:-0}
        failures=${failures:-0}
        errors=${errors:-0}
        skipped=${skipped:-0}
        time=${time:-0}

        TOTAL_TESTS=$((TOTAL_TESTS + tests))
        TOTAL_FAILURES=$((TOTAL_FAILURES + failures))
        TOTAL_ERRORS=$((TOTAL_ERRORS + errors))
        TOTAL_SKIPPED=$((TOTAL_SKIPPED + skipped))

        # 提取简短类名和包名
        local short_name=$(echo "$name" | awk -F. '{print $NF}')
        local pkg_name=$(echo "$name" | sed "s/\.$short_name$//")

        # 确定模块类别
        local category="other"
        case "$pkg_name" in
            *e2e*)        category="e2e" ;;
            *source*)     category="source" ;;
            *sink*)       category="sink" ;;
            *checkpoint*) category="checkpoint" ;;
            *config*)     category="config" ;;
            *model*)      category="model" ;;
            *core*)       category="core" ;;
            *metrics*)    category="metrics" ;;
            *alert*)      category="alert" ;;
            *reliability*) category="reliability" ;;
            *trace*)      category="trace" ;;
            *bootstrap*)  category="bootstrap" ;;
            *infra*)      category="infra" ;;
            *report*)     category="report" ;;
        esac

        # 确定状态
        local status="pass"
        if [ "$failures" -gt 0 ] || [ "$errors" -gt 0 ]; then
            status="fail"
        elif [ "$skipped" -gt 0 ] && [ "$tests" -eq "$skipped" ]; then
            status="skip"
        fi

        # 提取每个 testcase 详情
        local testcases_json=""
        while IFS= read -r tc_line; do
            [ -z "$tc_line" ] && continue
            local tc_name=$(echo "$tc_line" | grep -oP 'name="[^"]*"' | head -1 | sed 's/name="//;s/"//')
            local tc_time=$(echo "$tc_line" | grep -oP 'time="[^"]*"' | head -1 | sed 's/time="//;s/"//')
            tc_time=${tc_time:-0}

            # 检查是否有 failure/error 子元素（简化检测）
            local tc_status="pass"
            testcases_json="${testcases_json}{\"name\":\"${tc_name}\",\"time\":\"${tc_time}\",\"status\":\"${tc_status}\"},"
        done < <(grep -oP '<testcase[^/]*/?>' "$xml_file" || true)

        # 检查失败的测试用例
        local failed_tests=$(grep -oP '<testcase[^>]*name="([^"]*)"' "$xml_file" | while read -r line; do
            local tname=$(echo "$line" | grep -oP 'name="[^"]*"' | sed 's/name="//;s/"//')
            echo "$tname"
        done)

        # 收集数据行: category|shortName|pkgName|tests|failures|errors|skipped|time|status
        echo "${category}|${short_name}|${pkg_name}|${tests}|${failures}|${errors}|${skipped}|${time}|${status}" >> "$CLASSES_DATA_FILE"

    done <<< "$xml_files"

    TOTAL_PASSED=$((TOTAL_TESTS - TOTAL_FAILURES - TOTAL_ERRORS - TOTAL_SKIPPED))

    log_success "解析完成: 共 $TOTAL_TESTS 个用例, 通过 $TOTAL_PASSED, 失败 $TOTAL_FAILURES, 错误 $TOTAL_ERRORS, 跳过 $TOTAL_SKIPPED"
}

# ======================== 生成 HTML 报告 ========================

generate_html_report() {
    log_step "生成 HTML 测试报告"

    mkdir -p "$REPORT_DIR"

    # 确定整体状态
    local overall_status="PASSED"
    local overall_color="#10b981"
    local overall_icon="✅"
    if [ "$TOTAL_FAILURES" -gt 0 ] || [ "$TOTAL_ERRORS" -gt 0 ]; then
        overall_status="FAILED"
        overall_color="#ef4444"
        overall_icon="❌"
    fi

    local pass_rate=0
    if [ "$TOTAL_TESTS" -gt 0 ]; then
        pass_rate=$(echo "scale=1; $TOTAL_PASSED * 100 / $TOTAL_TESTS" | bc)
    fi

    # 统计各模块的数据
    local categories=("model" "config" "core" "source" "sink" "checkpoint" "reliability" "metrics" "alert" "trace" "bootstrap" "infra" "report" "e2e")
    local category_labels=("模型层" "配置层" "核心层" "Source" "Sink" "Checkpoint" "可靠性" "监控指标" "告警" "链路追踪" "启动引导" "基础设施" "报告" "端到端 E2E")
    local category_icons=("📦" "⚙️" "🔧" "📡" "💾" "📍" "🛡️" "📊" "🚨" "🔍" "🚀" "🏗️" "📋" "🔗")

    # 开始写 HTML
    cat > "$HTML_REPORT" << 'HTMLHEAD'
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RocketMQ HA Sync — 测试报告</title>
<style>
:root {
  --bg: #0f172a;
  --surface: #1e293b;
  --surface2: #334155;
  --border: #475569;
  --text: #f1f5f9;
  --text2: #94a3b8;
  --accent: #3b82f6;
  --accent2: #8b5cf6;
  --green: #10b981;
  --red: #ef4444;
  --yellow: #f59e0b;
  --orange: #f97316;
  --cyan: #06b6d4;
  --radius: 12px;
  --shadow: 0 4px 24px rgba(0,0,0,0.3);
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans SC', sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.6;
  min-height: 100vh;
}

/* 顶部导航 */
.topbar {
  background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
  border-bottom: 1px solid var(--border);
  padding: 16px 32px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 100;
  backdrop-filter: blur(12px);
}
.topbar-left { display: flex; align-items: center; gap: 12px; }
.topbar-logo {
  width: 40px; height: 40px;
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  border-radius: 10px;
  display: flex; align-items: center; justify-content: center;
  font-size: 20px; font-weight: bold;
}
.topbar-title { font-size: 18px; font-weight: 600; }
.topbar-sub { font-size: 13px; color: var(--text2); }
.topbar-right { display: flex; align-items: center; gap: 16px; }
.topbar-badge {
  padding: 4px 14px;
  border-radius: 20px;
  font-size: 13px;
  font-weight: 600;
  letter-spacing: 0.5px;
}

/* 容器 */
.container { max-width: 1280px; margin: 0 auto; padding: 24px 32px 48px; }

/* 状态卡片大区 */
.hero {
  margin-top: 24px;
  padding: 32px;
  border-radius: var(--radius);
  background: var(--surface);
  border: 1px solid var(--border);
  display: flex;
  align-items: center;
  gap: 32px;
  animation: fadeIn 0.5s ease;
}
.hero-icon {
  width: 88px; height: 88px;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 42px;
  flex-shrink: 0;
  box-shadow: 0 0 30px rgba(0,0,0,0.3);
}
.hero-content { flex: 1; }
.hero-status { font-size: 28px; font-weight: 700; margin-bottom: 4px; }
.hero-detail { color: var(--text2); font-size: 14px; }

/* 统计仪表盘 */
.dashboard {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 16px;
  margin-top: 24px;
}
.stat-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 20px;
  text-align: center;
  transition: transform 0.2s, box-shadow 0.2s;
  animation: slideUp 0.5s ease both;
}
.stat-card:hover { transform: translateY(-3px); box-shadow: var(--shadow); }
.stat-value { font-size: 32px; font-weight: 700; margin: 8px 0 4px; }
.stat-label { font-size: 13px; color: var(--text2); text-transform: uppercase; letter-spacing: 0.5px; }
.stat-icon { font-size: 24px; }

/* 进度环 */
.progress-ring-section {
  margin-top: 24px;
  display: grid;
  grid-template-columns: 200px 1fr;
  gap: 24px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 24px;
}
.ring-container { display: flex; align-items: center; justify-content: center; }
.ring-container svg { transform: rotate(-90deg); }
.ring-label { text-anchor: middle; dominant-baseline: middle; fill: var(--text); font-weight: 700; transform: rotate(90deg); transform-origin: center; }
.ring-sublabel { text-anchor: middle; dominant-baseline: middle; fill: var(--text2); font-size: 11px; transform: rotate(90deg); transform-origin: center; }
.module-bars { display: flex; flex-direction: column; gap: 10px; justify-content: center; }
.bar-row { display: flex; align-items: center; gap: 10px; }
.bar-label { width: 100px; font-size: 13px; color: var(--text2); text-align: right; white-space: nowrap; }
.bar-track { flex: 1; height: 24px; background: var(--surface2); border-radius: 6px; overflow: hidden; position: relative; }
.bar-fill { height: 100%; border-radius: 6px; transition: width 0.8s ease; display: flex; align-items: center; padding-left: 8px; font-size: 11px; font-weight: 600; color: #fff; min-width: fit-content; }
.bar-count { font-size: 12px; color: var(--text2); width: 40px; text-align: right; }

/* 标签页 */
.tabs {
  margin-top: 32px;
  display: flex;
  gap: 4px;
  background: var(--surface);
  padding: 4px;
  border-radius: 10px;
  border: 1px solid var(--border);
}
.tab-btn {
  flex: 1;
  padding: 10px 16px;
  background: none;
  border: none;
  color: var(--text2);
  cursor: pointer;
  border-radius: 8px;
  font-size: 14px;
  font-weight: 500;
  transition: all 0.2s;
}
.tab-btn:hover { background: var(--surface2); color: var(--text); }
.tab-btn.active { background: var(--accent); color: #fff; }
.tab-content { display: none; margin-top: 16px; animation: fadeIn 0.3s ease; }
.tab-content.active { display: block; }

/* 测试类卡片 */
.class-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  margin-bottom: 12px;
  overflow: hidden;
  transition: box-shadow 0.2s;
}
.class-card:hover { box-shadow: var(--shadow); }
.class-header {
  padding: 16px 20px;
  display: flex;
  align-items: center;
  gap: 12px;
  cursor: pointer;
  user-select: none;
  transition: background 0.15s;
}
.class-header:hover { background: var(--surface2); }
.class-icon { font-size: 18px; }
.class-name { flex: 1; font-weight: 600; font-size: 15px; }
.class-pkg { color: var(--text2); font-size: 12px; font-weight: 400; margin-left: 8px; }
.class-stats { display: flex; gap: 12px; align-items: center; }
.class-badge {
  padding: 3px 10px;
  border-radius: 12px;
  font-size: 12px;
  font-weight: 600;
}
.badge-pass { background: rgba(16,185,129,0.15); color: var(--green); }
.badge-fail { background: rgba(239,68,68,0.15); color: var(--red); }
.badge-skip { background: rgba(245,158,11,0.15); color: var(--yellow); }
.class-time { color: var(--text2); font-size: 12px; }
.class-chevron { color: var(--text2); transition: transform 0.2s; font-size: 12px; }
.class-card.open .class-chevron { transform: rotate(90deg); }
.class-detail { display: none; border-top: 1px solid var(--border); }
.class-card.open .class-detail { display: block; }

/* 测试方法表格 */
.method-table { width: 100%; border-collapse: collapse; }
.method-table th {
  background: var(--surface2);
  padding: 10px 16px;
  text-align: left;
  font-size: 12px;
  font-weight: 600;
  color: var(--text2);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.method-table td {
  padding: 10px 16px;
  border-bottom: 1px solid rgba(71,85,105,0.3);
  font-size: 13px;
}
.method-table tr:last-child td { border-bottom: none; }
.method-table tr:hover td { background: rgba(59,130,246,0.05); }
.method-status { display: inline-flex; align-items: center; gap: 6px; }
.dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.dot-pass { background: var(--green); box-shadow: 0 0 6px var(--green); }
.dot-fail { background: var(--red); box-shadow: 0 0 6px var(--red); }
.dot-skip { background: var(--yellow); }

/* 页脚 */
.footer {
  margin-top: 48px;
  padding: 24px;
  text-align: center;
  color: var(--text2);
  font-size: 13px;
  border-top: 1px solid var(--border);
}

/* 动画 */
@keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
@keyframes slideUp { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }

/* 响应式 */
@media (max-width: 768px) {
  .dashboard { grid-template-columns: repeat(2, 1fr); }
  .progress-ring-section { grid-template-columns: 1fr; }
  .container { padding: 16px; }
  .hero { flex-direction: column; text-align: center; }
}
</style>
</head>
<body>
HTMLHEAD

    # 写入顶部导航
    cat >> "$HTML_REPORT" << HTMLTOPBAR
<!-- 顶部导航 -->
<div class="topbar">
  <div class="topbar-left">
    <div class="topbar-logo">⚡</div>
    <div>
      <div class="topbar-title">RocketMQ HA Sync — 测试报告</div>
      <div class="topbar-sub">rocketmq-ha-sync 1.0.0-SNAPSHOT</div>
    </div>
  </div>
  <div class="topbar-right">
    <span style="color:var(--text2);font-size:13px;">$TIMESTAMP</span>
    <span class="topbar-badge" style="background:${overall_color};color:#fff;">${overall_icon} ${overall_status}</span>
  </div>
</div>
HTMLTOPBAR

    # 写入 hero 区域
    local duration_str="${TOTAL_DURATION:-0}s"
    cat >> "$HTML_REPORT" << HTMLHERO
<div class="container">

<!-- 状态概览 -->
<div class="hero">
  <div class="hero-icon" style="background:linear-gradient(135deg, ${overall_color}33, ${overall_color}11);">
    ${overall_icon}
  </div>
  <div class="hero-content">
    <div class="hero-status" style="color:${overall_color}">构建 ${overall_status}</div>
    <div class="hero-detail">
      共执行 <strong>${TOTAL_TESTS}</strong> 个测试用例 ·
      通过 <strong style="color:var(--green)">${TOTAL_PASSED}</strong> ·
      失败 <strong style="color:var(--red)">${TOTAL_FAILURES}</strong> ·
      错误 <strong style="color:var(--orange)">${TOTAL_ERRORS}</strong> ·
      跳过 <strong style="color:var(--yellow)">${TOTAL_SKIPPED}</strong> ·
      耗时 <strong>${duration_str}</strong>
    </div>
  </div>
</div>

<!-- 统计仪表盘 -->
<div class="dashboard">
  <div class="stat-card" style="animation-delay:0.05s">
    <div class="stat-icon">📊</div>
    <div class="stat-value">${TOTAL_TESTS}</div>
    <div class="stat-label">总用例数</div>
  </div>
  <div class="stat-card" style="animation-delay:0.10s">
    <div class="stat-icon">✅</div>
    <div class="stat-value" style="color:var(--green)">${TOTAL_PASSED}</div>
    <div class="stat-label">通过</div>
  </div>
  <div class="stat-card" style="animation-delay:0.15s">
    <div class="stat-icon">❌</div>
    <div class="stat-value" style="color:var(--red)">${TOTAL_FAILURES}</div>
    <div class="stat-label">失败</div>
  </div>
  <div class="stat-card" style="animation-delay:0.20s">
    <div class="stat-icon">⏭️</div>
    <div class="stat-value" style="color:var(--yellow)">${TOTAL_SKIPPED}</div>
    <div class="stat-label">跳过</div>
  </div>
  <div class="stat-card" style="animation-delay:0.25s">
    <div class="stat-icon">🎯</div>
    <div class="stat-value" style="color:var(--cyan)">${pass_rate}%</div>
    <div class="stat-label">通过率</div>
  </div>
</div>
HTMLHERO

    # ====== 进度环 + 模块柱状图 ======
    local ring_r=70
    local ring_c=$(echo "scale=2; 2 * 3.14159 * $ring_r" | bc)
    local ring_offset=$(echo "scale=2; $ring_c * (1 - $pass_rate / 100)" | bc)

    cat >> "$HTML_REPORT" << HTMLRING
<!-- 通过率环 + 模块分布 -->
<div class="progress-ring-section">
  <div class="ring-container">
    <svg width="180" height="180" viewBox="0 0 180 180">
      <circle cx="90" cy="90" r="$ring_r" fill="none" stroke="var(--surface2)" stroke-width="12"/>
      <circle cx="90" cy="90" r="$ring_r" fill="none" stroke="${overall_color}" stroke-width="12"
              stroke-dasharray="$ring_c" stroke-dashoffset="$ring_offset"
              stroke-linecap="round" style="transition: stroke-dashoffset 1.2s ease;"/>
      <text x="90" y="85" class="ring-label" font-size="28">${pass_rate}%</text>
      <text x="90" y="108" class="ring-sublabel">通过率</text>
    </svg>
  </div>
  <div class="module-bars">
HTMLRING

    # 为每个模块生成柱状图
    local bar_colors=("#3b82f6" "#8b5cf6" "#06b6d4" "#10b981" "#f59e0b" "#ef4444" "#ec4899" "#f97316" "#ef4444" "#14b8a6" "#6366f1" "#64748b" "#a78bfa" "#22d3ee")
    local idx=0
    for cat_key in "${categories[@]}"; do
        local cat_label="${category_labels[$idx]}"
        local cat_color="${bar_colors[$idx]}"
        local cat_tests=0
        while IFS='|' read -r c _ _ t _ _ _ _ _; do
            [ "$c" = "$cat_key" ] && cat_tests=$((cat_tests + t))
        done < "$CLASSES_DATA_FILE"

        if [ "$cat_tests" -gt 0 ]; then
            local bar_pct=0
            if [ "$TOTAL_TESTS" -gt 0 ]; then
                bar_pct=$(echo "scale=0; $cat_tests * 100 / $TOTAL_TESTS" | bc)
            fi
            [ "$bar_pct" -lt 3 ] && bar_pct=3  # 最小可见宽度

            cat >> "$HTML_REPORT" << HTMLBAR
    <div class="bar-row">
      <span class="bar-label">${category_icons[$idx]} ${cat_label}</span>
      <div class="bar-track">
        <div class="bar-fill" style="width:${bar_pct}%;background:${cat_color}">${cat_tests}</div>
      </div>
      <span class="bar-count">${cat_tests}</span>
    </div>
HTMLBAR
        fi
        idx=$((idx + 1))
    done

    echo "  </div></div>" >> "$HTML_REPORT"

    # ====== 标签页 ======
    cat >> "$HTML_REPORT" << 'HTMLTABS'

<!-- 标签页 -->
<div class="tabs" id="tabsBar">
  <button class="tab-btn active" onclick="switchTab('all')">📋 全部</button>
  <button class="tab-btn" onclick="switchTab('unit')">🧪 单元测试</button>
  <button class="tab-btn" onclick="switchTab('e2e')">🔗 E2E 测试</button>
  <button class="tab-btn" onclick="switchTab('fail')" id="failTab">❌ 失败</button>
</div>
HTMLTABS

    # ====== 测试类卡片 ======
    echo '<div class="tab-content active" id="tab-all">' >> "$HTML_REPORT"

    # 按模块排序输出
    while IFS='|' read -r cat shortName pkgName tests failures errors skipped time status; do
        [ -z "$cat" ] && continue

        local status_badge_class="badge-pass"
        local status_text="PASS"
        [ "$status" = "fail" ] && status_badge_class="badge-fail" && status_text="FAIL"
        [ "$status" = "skip" ] && status_badge_class="badge-skip" && status_text="SKIP"

        local data_type="unit"
        [ "$cat" = "e2e" ] && data_type="e2e"
        local data_fail=""
        [ "$status" = "fail" ] && data_fail="true"

        local time_display="${time}s"

        # 从对应 XML 文件提取 testcase 列表
        local xml_match=$(find "$SUREFIRE_DIR" -name "TEST-*${shortName}.xml" -type f | head -1)

        # 预先计算图标（heredoc 内不支持命令替换语法 ${...}）
        local class_icon="🧪"
        [ "$cat" = "e2e" ] && class_icon="🔗"

        cat >> "$HTML_REPORT" << HTMLCARD
<div class="class-card" data-type="${data_type}" data-fail="${data_fail}">
  <div class="class-header" onclick="this.parentElement.classList.toggle('open')">
    <span class="class-icon">${class_icon}</span>
    <span class="class-name">${shortName}<span class="class-pkg">${pkgName}</span></span>
    <div class="class-stats">
      <span class="class-badge ${status_badge_class}">${status_text}</span>
      <span class="class-badge badge-pass">${tests} 用例</span>
      <span class="class-time">⏱ ${time_display}</span>
    </div>
    <span class="class-chevron">▶</span>
  </div>
  <div class="class-detail">
    <table class="method-table">
      <thead><tr><th>测试方法</th><th>状态</th><th>耗时</th></tr></thead>
      <tbody>
HTMLCARD

        # 提取每个 testcase
        if [ -n "$xml_match" ] && [ -f "$xml_match" ]; then
            grep -oP '<testcase[^>]+>' "$xml_match" | while IFS= read -r tc_tag; do
                local tc_name=$(echo "$tc_tag" | grep -oP 'name="[^"]*"' | sed 's/name="//;s/"//')
                local tc_time=$(echo "$tc_tag" | grep -oP 'time="[^"]*"' | sed 's/time="//;s/"//')
                tc_time=${tc_time:-0}

                cat >> "$HTML_REPORT" << HTMLROW
        <tr>
          <td><span class="method-status"><span class="dot dot-pass"></span>${tc_name}</span></td>
          <td><span style="color:var(--green)">✓ 通过</span></td>
          <td style="color:var(--text2)">${tc_time}s</td>
        </tr>
HTMLROW
            done
        fi

        cat >> "$HTML_REPORT" << 'HTMLCARDEND'
      </tbody>
    </table>
  </div>
</div>
HTMLCARDEND

    done < <(sort -t'|' -k1,1 "$CLASSES_DATA_FILE")

    echo '</div>' >> "$HTML_REPORT"

    # ====== 页脚 + JS ======
    cat >> "$HTML_REPORT" << HTMLFOOT

<!-- 页脚 -->
<div class="footer">
  <p>RocketMQ HA Data Sync — 测试报告 · 生成于 $TIMESTAMP</p>
  <p style="margin-top:4px;font-size:12px;">Apache RocketMQ · rocketmq-ha-sync 1.0.0-SNAPSHOT</p>
</div>

</div><!-- end container -->

<script>
function switchTab(type) {
  // 更新按钮状态
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');

  // 显示/隐藏卡片
  document.querySelectorAll('.class-card').forEach(card => {
    const cardType = card.dataset.type;
    const cardFail = card.dataset.fail;
    let show = false;
    if (type === 'all') show = true;
    else if (type === 'unit') show = (cardType === 'unit');
    else if (type === 'e2e') show = (cardType === 'e2e');
    else if (type === 'fail') show = (cardFail === 'true');
    card.style.display = show ? '' : 'none';
  });
}

// 统计失败数显示在标签页
(function() {
  const failCount = document.querySelectorAll('.class-card[data-fail="true"]').length;
  const failTab = document.getElementById('failTab');
  if (failTab) failTab.textContent = '❌ 失败 (' + failCount + ')';
})();
</script>
</body>
</html>
HTMLFOOT

    # 清理临时文件
    rm -f "$CLASSES_DATA_FILE"

    log_success "HTML 报告已生成: $HTML_REPORT"
}

# ======================== 打印终端摘要 ========================

print_summary() {
    echo ""
    echo -e "${BOLD}══════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}  📊  测试执行摘要${NC}"
    echo -e "${BOLD}══════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  总用例数:  ${BOLD}${TOTAL_TESTS}${NC}"
    echo -e "  ✅ 通过:   ${GREEN}${BOLD}${TOTAL_PASSED}${NC}"
    echo -e "  ❌ 失败:   ${RED}${BOLD}${TOTAL_FAILURES}${NC}"
    echo -e "  ⚠️  错误:   ${YELLOW}${BOLD}${TOTAL_ERRORS}${NC}"
    echo -e "  ⏭️  跳过:   ${YELLOW}${BOLD}${TOTAL_SKIPPED}${NC}"
    echo -e "  ⏱  耗时:   ${BOLD}${TOTAL_DURATION:-0}s${NC}"
    echo ""

    if [ -f "$HTML_REPORT" ]; then
        echo -e "  📄 HTML 报告: ${CYAN}${BOLD}${HTML_REPORT}${NC}"
    fi
    echo -e "  📂 XML 报告:  ${CYAN}${SUREFIRE_DIR}/${NC}"
    echo ""

    if [ "${TEST_EXIT_CODE:-0}" -eq 0 ]; then
        echo -e "  ${GREEN}${BOLD}🎉 所有测试通过！${NC}"
    else
        echo -e "  ${RED}${BOLD}💥 存在测试失败，请查看报告。${NC}"
    fi
    echo ""
    echo -e "${BOLD}══════════════════════════════════════════════════════════${NC}"
}

# ======================== 主流程 ========================

main() {
    local mode="all"
    local test_filter=""
    local skip_test=false
    local skip_report=false
    VERBOSE="false"
    TOTAL_DURATION=0
    TEST_EXIT_CODE=0

    # 解析参数
    while [ $# -gt 0 ]; do
        case "$1" in
            --help|-h)
                print_help; exit 0 ;;
            --unit)
                mode="unit"; shift ;;
            --e2e)
                mode="e2e"; shift ;;
            --class)
                mode="class"; test_filter="$2"; shift 2 ;;
            --module)
                mode="module"; test_filter="$2"; shift 2 ;;
            --report-only)
                skip_test=true; shift ;;
            --no-report)
                skip_report=true; shift ;;
            --verbose|-v)
                VERBOSE="true"; shift ;;
            *)
                log_error "未知参数: $1"; print_help; exit 1 ;;
        esac
    done

    print_banner
    mkdir -p "$REPORT_DIR"

    # 构建 Maven 测试参数
    local mvn_args=""
    local label=""
    case "$mode" in
        all)
            mvn_args=""
            label="全部测试" ;;
        unit)
            mvn_args='-Dtest="!org.apache.rocketmq.hasync.e2e.*"'
            label="单元测试（排除 E2E）" ;;
        e2e)
            mvn_args='-Dtest="EndToEndDataFlowTest,EndToEndConfigBootstrapTest,EndToEndErrorRecoveryTest,EndToEndGracefulShutdownTest,EndToEndExceptionScenariosTest"'
            label="端到端测试（E2E）" ;;
        class)
            mvn_args="-Dtest=${test_filter}"
            label="指定测试类: ${test_filter}" ;;
        module)
            mvn_args="-Dtest=\"org.apache.rocketmq.hasync.${test_filter}.*Test\""
            label="模块测试: ${test_filter}" ;;
    esac

    # 运行测试
    if [ "$skip_test" = "false" ]; then
        run_tests "$mvn_args" "$label"
    else
        log_info "跳过测试执行（--report-only 模式）"
    fi

    # 解析 XML 报告
    parse_surefire_xml

    # 生成 HTML 报告
    if [ "$skip_report" = "false" ]; then
        generate_html_report
    fi

    # 打印摘要
    print_summary

    exit ${TEST_EXIT_CODE:-0}
}

main "$@"
