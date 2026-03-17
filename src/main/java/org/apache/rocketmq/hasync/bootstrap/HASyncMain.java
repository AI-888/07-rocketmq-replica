package org.apache.rocketmq.hasync.bootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HA Sync 统一入口 — 根据 --mode 参数分发到 Source 或 Sink 进程
 * <p>
 * <b>重要约束（需求 3 — 不参与选举）：</b>
 * <ul>
 *   <li>本组件以「虚拟 Slave」身份连接 Master，仅接收数据，不参与任何选举或投票</li>
 *   <li>不注册为合法 Slave 副本</li>
 *   <li>不响应 Controller 的选举请求</li>
 *   <li>不干扰源集群的 ISR/OSR 和多副本机制</li>
 * </ul>
 * <p>
 * 用法：
 * <pre>
 * java -jar ha-sync.jar --mode source --sourceNamesrv addr --targetNamesrv addr
 * java -jar ha-sync.jar --mode sink --targetNamesrv addr
 * </pre>
 */
public class HASyncMain {

    private static final Logger log = LoggerFactory.getLogger(HASyncMain.class);

    public static void main(String[] args) {
        String mode = extractMode(args);

        if (mode == null) {
            printUsageAndExit();
            return;
        }

        switch (mode.toLowerCase()) {
            case "source":
                log.info("启动模式: Source");
                SourceBootstrap.main(stripModeArgs(args));
                break;
            case "sink":
                log.info("启动模式: Sink");
                SinkBootstrap.main(stripModeArgs(args));
                break;
            default:
                log.error("未知的运行模式: {}，支持的模式: source, sink", mode);
                printUsageAndExit();
        }
    }

    /**
     * 从命令行参数中提取 --mode 的值
     */
    public static String extractMode(String[] args) {
        if (args == null) {
            return null;
        }
        for (int i = 0; i < args.length; i++) {
            if ("--mode".equals(args[i]) && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return null;
    }

    /**
     * 去除 --mode 参数后返回剩余参数
     */
    public static String[] stripModeArgs(String[] args) {
        if (args == null) {
            return new String[0];
        }
        java.util.List<String> result = new java.util.ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("--mode".equals(args[i]) && i + 1 < args.length) {
                i++; // 跳过 --mode 及其值
            } else {
                result.add(args[i]);
            }
        }
        return result.toArray(new String[0]);
    }

    private static void printUsageAndExit() {
        System.err.println("用法: java -jar ha-sync.jar --mode <source|sink> [选项]");
        System.err.println();
        System.err.println("运行模式:");
        System.err.println("  source  启动 Source 进程（从源集群拉取数据）");
        System.err.println("  sink    启动 Sink 进程（向目标集群写入数据）");
        System.err.println();
        System.err.println("示例:");
        System.err.println("  java -jar ha-sync.jar --mode source --sourceNamesrv 127.0.0.1:9876 --targetNamesrv 127.0.0.1:9877");
        System.err.println("  java -jar ha-sync.jar --mode sink --targetNamesrv 127.0.0.1:9877");
        System.exit(1);
    }
}
