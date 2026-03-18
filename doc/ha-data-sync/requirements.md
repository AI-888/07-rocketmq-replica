# 需求文档：RocketMQ HA 数据同步组件

## 引言

本组件是一个独立的 Java 程序，用于模拟 RocketMQ Slave Broker 的主从复制行为，从存储层角度实现数据同步。该组件通过 NameServer 发现当前集群的 Master Broker，伪装成 Slave 节点，使用 RocketMQ 原生的 HA 复制协议（DefaultHAService 协议）与 Master 建立 TCP 连接，持续拉取 CommitLog 数据并写入目标 RocketMQ 集群。

**架构设计（Source 无状态多实例 + Sink 分布式）：**

本组件采用类似 **Flink Connector** 的 Source/Sink 分离架构：

```
┌──────────────────────────────────────────────────────────────────────┐
│                      Source Worker（无状态，可多实例）                 │
│                                                                      │
│  ┌──────────────────┐   ZMQ REP Socket   ┌──────────────────────┐   │
│  │   HASource        │ ◄─── REQ-REP ───► │  Sink（分布式多节点）  │  │
│  │  (无状态，拉取)   │   PullReq/Resp    │  (写入目标 RocketMQ)  │  │
│  └──────────────────┘                     └──────────────────────┘  │
│           │                                          │               │
│    CheckpointCoordinator（位点协调器）◄───────────────┘               │
└──────────────────────────────────────────────────────────────────────┘
```

- **HASource**：负责连接 Master、接收 CommitLog 数据、解析消息，统计每个 Topic 的消息字节数，将解析后的 `SyncRecord` 暂存于本地内存缓冲区，通过 ZMQ REP Socket 向 Sink 提供数据；Source 本身无状态，故障后由外部工具自动拉起，从 Checkpoint 断点续传。可能存在多个 Source，每个 Source 将自身信息写入源集群 NameServer 的独立唯一 KV 中
- **Sink（分布式）**：通过 ZMQ REQ Socket 从 Source 拉取数据，Source 将流量分发给已注册的 Sink 列表；多个 Sink 节点并行工作
- **SyncRecord**：Source 与 Sink 之间传递的数据单元，包含消息内容、偏移量、Topic、字节数等元信息
- **CheckpointCoordinator**：协调 Source 与 Sink 的位点，仅在 Sink 确认写入落盘后才推进 `confirmedOffset`

> **统一通信模型**：Source 与 Sink 之间**统一**通过 ZeroMQ REQ-REP 模式通信，无论是独立部署还是同进程启动，均使用相同的通信、发现逻辑，确保行为完全一致，减少维护差异。Source 启动时可通过 `--with-sink` 参数在同一进程内嵌启动 Sink 实例，此时 Sink 通过 `localhost` ZMQ 连接 Source，通信协议与独立部署完全相同。

**核心特性：**
- Source/Sink 解耦，Source 专注数据拉取与流量统计，Sink 专注数据写入，可独立扩展
- **统一 ZMQ 通信模型**：Source 和 Sink **始终**通过 ZeroMQ（REQ-REP 模式）通信，无论是独立部署还是同进程启动，通信协议和服务发现逻辑完全一致，减少维护差异
- **灵活部署模式**：Source 和 Sink 可作为独立 Worker 进程运行（独立部署），也可通过 `--with-sink` 参数让 Source 在同一进程内嵌启动 Sink（同进程模式），两种模式下 Sink 均通过 ZMQ 从 Source 拉取数据
- **Source 地址注册**：每个 Source 启动后将自身 ZMQ 地址注册到源集群 NameServer 的独立唯一 KV 中（key 为 `{sourceNodeId}`），支持多个 Source 同时存在；Sink 通过源集群 NameServer KV 自动发现可用 Source 列表
- **完全无状态设计**：Checkpoint 等状态信息存储在目标集群 NameServer KV 中，Source 和 Sink 均无本地状态，可随意迁移和替换
- 通过 NameServer 动态发现 Master Broker 的 HA 地址，Master 切换后自动重连
- 不参与主从切换选举，仅作为只读数据复制节点
- **最终一致性**语义保证（At-Least-Once + 启动一致性校验）+ 不丢消息保证
- **消息顺序严格一致性**：Topic 配置保持一致，消息按照源集群 CommitLog 物理偏移量顺序写入目标集群，保证 Source 获取到的消息顺序与 Sink 写入目标的顺序严格一致
- 同步位点（Checkpoint）持久化，支持断点续传
- 支持按 Topic 过滤，支持全量元数据同步
- 完善的异常处理：Master 宕机自动重连、CommitLog 过期检测、消息完整性校验
- 全链路 Trace 跟踪 + 丰富监控指标
- 自动重试（网络抖动容错）
- 启动时兼容性预校验

**开发阶段划分：**

| 阶段 | 需求编号 | 内容 |
|------|---------|------|
| 阶段一：基础骨架 | 需求 1~3 | 启动参数配置、核心接口定义（Source/Sink 独立部署 + ZeroMQ 通信）、不参与选举约束 |
| 阶段二：Source 核心 | 需求 4~8 | NameServer 发现、兼容性预校验、CommitLog 过期检测、消息完整性校验、Master 切换重连 |
| 阶段三：Checkpoint + 最终一致性 | 需求 9~10 | 位点持久化、最终一致性语义 + 启动一致性校验 |
| 阶段四：Sink 核心 | 需求 11~13 | Topic 过滤、元数据同步、RFQ 副本失败队列 |
| 阶段五：可靠性增强 | 需求 14~17 | 异常处理、自动重试、目标不可写监控、优雅停机 |
| 阶段六：分布式 & 高性能 | 需求 18~19 | Source 多实例 + Sink 分布式、全链路 Trace + 高 TPS |
| 阶段七：可观测性 | 需求 20 | 监控指标 |

---

## 需求

---

## 阶段一：基础骨架

### 需求 1：启动参数配置

**用户故事：** 作为一名运维人员，我希望通过命令行参数、配置文件或环境变量灵活配置数据同步组件（Source 和 Sink 独立进程）的启动行为，以便在不同环境中快速部署。Source 和 Sink 分别拥有独立的启动参数集，由各自的 `SourceConfig` / `SinkConfig` 统一解析（支持三种配置来源，优先级：**环境变量 > 命令行参数 > 配置文件**）。

#### 验收标准

**Source 启动参数（ha-sync-source 进程）：**

1. WHEN 启动 Source 组件时 THEN 系统 SHALL 支持以下**必填**启动参数：

   | 参数 | 说明 |
   |------|------|
   | `--sourceNamesrv <addr>` | 源集群 NameServer 地址，多个地址以 `;` 分隔 |
   | `--targetNamesrv <addr>` | 目标集群 NameServer 地址（用于写入 Checkpoint 等） |

2. WHEN 启动 Source 组件时 THEN 系统 SHALL 支持以下**可选**启动参数：

   | 参数 | 默认值 | 说明 |
   |------|--------|------|
   | `--sourceMetricsPort <port>` | `9876` | Source HTTP 监控端口 |
   | `--heartbeatInterval <ms>` | `5000` | 向 Master 上报偏移量的间隔 |
   | `--masterPollInterval <ms>` | `30000` | 轮询 NameServer 检测 Master 变更的间隔 |
   | `--checkpointFlushInterval <ms>` | `1000` | Checkpoint 刷写间隔 |
   | `--checkpointFlushBatchSize <n>` | `100` | 累计 n 个数据包触发 Checkpoint 刷写 |
   | `--sourceNodeId <id>` | `hostname:pid` | Source 节点标识 |
   | `--zmqBindPort <port>` | `5555` | ZeroMQ REP Socket 绑定端口，Sink 通过此端口拉取数据 |
   | `--rfqTopic <topic>` | `ha-sync-rfq` | RFQ 专用 Topic 名称（写入源集群） |
   | `--rfqProducerGroup <group>` | `ha-sync-rfq-producer` | RFQ Producer Group 名称 |
   | `--rfqMaxRetry <n>` | `3` | RFQ 消息发送失败最大重试次数 |
   | `--parseErrorSuspendWindowMs <ms>` | `60000` | 解析失败暂停检测的滑动窗口时长 |
   | `--metaSyncInterval <ms>` | `60000` | 元数据同步间隔 |
   | `--with-sink` | `false` | 是否在 Source 同进程内嵌启动 Sink 实例。指定后 Source 进程内自动创建 Sink，Sink 通过 `localhost:{zmqPort}` 连接 Source ZMQ REP Socket，通信和服务发现逻辑与独立部署完全一致 |

**Sink 启动参数（ha-sync-sink 进程）：**

3. WHEN 启动 Sink 组件时 THEN 系统 SHALL 支持以下**必填**启动参数：

   | 参数 | 说明 |
   |------|------|
   | `--targetNamesrv <addr>` | 目标集群 NameServer 地址（用于写入消息、查询 Source 地址、读写 Checkpoint 等） |

4. WHEN 启动 Sink 组件时 THEN 系统 SHALL 支持以下**可选**启动参数：

   | 参数 | 默认值 | 说明 |
   |------|--------|------|
   | `--sinkMetricsPort <port>` | `9877` | Sink HTTP 监控端口 |
   | `--sinkId <id>` | `hostname:pid` | Sink 节点唯一标识，用于在目标集群 NameServer KV 中标识 commitOffset |
   | `--sinkBatchSize <n>` | `100` | Sink 批量发送大小 |
   | `--sinkThreads <n>` | `4` | Sink 并发写入线程数 |
   | `--sinkMaxRetry <n>` | `3` | Sink 写入失败最大重试次数 |
   | `--targetProbeInterval <ms>` | `30000` | 目标集群探活间隔 |
   | `--startupCheckMsgCount <n>` | `10` | 启动一致性校验消息条数（0=跳过校验） |
   | `--topicSyncMaxRetry <n>` | `3` | 按需 Topic 同步到目标集群的最大重试次数 |
**配置文件支持：**

5. WHEN 启动 Source 或 Sink 组件时 THEN 系统 SHALL 支持通过 `--configFile <path>` 参数指定配置文件路径（可选，Source 默认查找 `./ha-sync-source.properties`，Sink 默认查找 `./ha-sync-sink.properties`，文件不存在时忽略）；配置文件采用标准 Java Properties 格式（`key=value`，每行一个配置项），配置项 key 与命令行参数名称一致（去掉 `--` 前缀），例如：

   **Source 配置文件示例（ha-sync-source.properties）：**
   ```properties
   sourceNamesrv=192.168.1.10:9876;192.168.1.11:9876
   targetNamesrv=10.0.0.10:9876
   sourceMetricsPort=9876
   heartbeatInterval=5000
   zmqBindPort=5555
   rfqTopic=ha-sync-rfq
   metaSyncInterval=60000
   ```

   **Sink 配置文件示例（ha-sync-sink.properties）：**
   ```properties
   targetNamesrv=10.0.0.10:9876
   sinkMetricsPort=9877
   sinkId=sink-node-01
   sinkBatchSize=200
   sinkThreads=8
   startupCheckMsgCount=20
   topicSyncMaxRetry=3
   ```
6. WHEN 配置文件存在时 THEN 系统 SHALL 在解析命令行参数之前先加载配置文件中的所有配置项作为默认值；命令行参数中显式指定的值将覆盖配置文件中的同名配置项
7. WHEN 配置文件中包含未识别的配置项 key 时 THEN 系统 SHALL 打印 WARN 日志（"未识别的配置项: {key}，已忽略"），但不影响启动
8. WHEN 配置文件格式错误（非法字符、编码异常等）时 THEN 系统 SHALL 打印 ERROR 日志并以非零退出码退出，提示配置文件解析失败及具体错误原因

**环境变量支持：**

9. WHEN 启动组件时 THEN 系统 SHALL 支持通过环境变量设置每个配置项；环境变量命名规则为：将配置项名称转换为**全大写 + 下划线分隔**，Source 参数添加 `HA_SOURCE_` 前缀，Sink 参数添加 `HA_SINK_` 前缀，例如：

   **Source 环境变量映射：**
   | 配置项 | 环境变量名 |
   |--------|-----------|
   | `sourceNamesrv` | `HA_SOURCE_SOURCE_NAMESRV` |
   | `targetNamesrv` | `HA_SOURCE_TARGET_NAMESRV` |
   | `sourceMetricsPort` | `HA_SOURCE_SOURCE_METRICS_PORT` |
   | `zmqBindPort` | `HA_SOURCE_ZMQ_BIND_PORT` |
   | `heartbeatInterval` | `HA_SOURCE_HEARTBEAT_INTERVAL` |
   | `metaSyncInterval` | `HA_SOURCE_META_SYNC_INTERVAL` |

   **Sink 环境变量映射：**
   | 配置项 | 环境变量名 |
   |--------|-----------|
   | `targetNamesrv` | `HA_SINK_TARGET_NAMESRV` |
   | `sinkMetricsPort` | `HA_SINK_SINK_METRICS_PORT` |
   | `sinkId` | `HA_SINK_SINK_ID` |
   | `sinkBatchSize` | `HA_SINK_SINK_BATCH_SIZE` |
   | `sinkThreads` | `HA_SINK_SINK_THREADS` |
   | `startupCheckMsgCount` | `HA_SINK_STARTUP_CHECK_MSG_COUNT` |
   | `topicSyncMaxRetry` | `HA_SINK_TOPIC_SYNC_MAX_RETRY` |

10. WHEN 环境变量、命令行参数和配置文件同时存在同一配置项时 THEN 系统 SHALL 按以下优先级加载（高优先级覆盖低优先级）：
    - **最高优先级**：环境变量（`HA_SOURCE_*` / `HA_SINK_*`）
    - **中优先级**：命令行参数（`--key value`）
    - **最低优先级**：配置文件（`ha-sync-source.properties` / `ha-sync-sink.properties`）
11. WHEN `SourceConfig` / `SinkConfig` 完成配置加载后 THEN 系统 SHALL 在启动日志中打印所有**最终生效**的配置项及其来源（`ENV` / `CLI` / `FILE` / `DEFAULT`），敏感信息（如 NameServer 地址）仅打印部分掩码值（如 `192.168.*.***:9876`）
12. WHEN 配置项的值为空字符串时 THEN 系统 SHALL 视为未设置，回退到下一优先级的值或使用默认值

**参数校验：**

13. IF 必填参数缺失（经过三种配置来源合并后仍未提供）THEN 系统 SHALL 打印使用说明并以非零退出码退出
14. WHEN 组件收到 SIGTERM 或 SIGINT 信号时 THEN 系统 SHALL 优雅关闭（关闭 TCP 连接、刷新本地存储、停止后台线程），并在 30 秒内完成退出

---

### 需求 2：Source/Sink 架构设计

**用户故事：** 作为一名开发者，我希望同步任务被拆分为独立的 Source 任务和 Sink 任务，类似 Flink Connector 的设计，以便 Source 和 Sink 可以独立扩展、替换和测试。

#### 验收标准

1. WHEN 系统设计时 THEN 系统 SHALL 定义以下核心接口与数据模型：
   - **`SyncSource` 接口**：定义 Source 的生命周期方法（`start()`、`stop()`、`isRunning()`）和数据拉取行为（`poll()`），Source 负责从 Master 拉取数据并产出 `SyncRecord`
   - **`SyncSink` 接口**：定义 Sink 的生命周期方法（`start()`、`stop()`）和数据写入行为（`write(SyncRecord record)`、`flush()`），Sink 负责消费 `SyncRecord` 并写入目标 RocketMQ 集群
   - **`SyncRecord` 数据类**：Source 与 Sink 之间传递的数据单元，包含字段：`masterPhyOffset`（数据包起始偏移量）、`endOffset`（数据包结束偏移量）、`physicOffset`（消息物理偏移量，作为绝对顺序依据，**Sink 必须严格按此字段升序写入目标集群**）、`topic`（消息 Topic）、`queueId`（消息所属队列 ID）、`body`（消息体字节数组）、`msgSize`（消息大小字节数）、`storeTimestamp`（存储时间戳）、`receiveTimestamp`（接收时间戳）、`traceId`（全链路追踪 ID）
   - **`CheckpointCoordinator` 接口**：定义位点读取（`getConfirmedOffset()`）和推进（`commitOffset(long offset)`）方法，由 Sink 在写入落盘后调用 `commitOffset`

2. WHEN 系统设计时 THEN Source 和 Sink 之间 SHALL **统一**通过 ZeroMQ REQ-REP 模式通信（Sink 主动拉取），Source 将解析后的 `SyncRecord` 暂存于本地内存缓冲区，Sink 通过 ZMQ 拉取。不存在 BlockingQueue 直连模式，所有部署形态下通信逻辑完全一致。
   - Source 启动时可通过 `--with-sink` 参数指定是否在同一进程内嵌启动 Sink 实例
   - 同进程内嵌的 Sink 通过 `localhost:{zmqPort}` 连接 Source 的 ZMQ REP Socket，通信协议和服务发现与独立部署完全相同
   - 当 Source 或 Sink 任一发生不可恢复异常时，系统 SHALL 记录 ERROR 日志并停止相关组件

3. WHEN 系统设计时 THEN 系统 SHALL 实现 **`HASource`**（`SyncSource` 的具体实现），职责为：
   - 通过 NameServer 发现 Master HA 地址
   - 使用 DefaultHAService Slave 协议与 Master 建立 TCP 连接
   - 持续接收并解析 CommitLog 数据包，将每条消息封装为 `SyncRecord` 暂存于本地内存缓冲区
   - 通过 ZMQ REP Socket 响应 Sink 的 PullRequest，按需返回缓冲区中的 SyncRecord
   - 统计每个 Topic 的消息字节数（`topicBytesStats`），供监控使用
   - 处理 Master 宕机、网络闪断等异常，自动重连
   - **不执行 Topic 过滤和存储写入**，这些职责属于 Sink

4. WHEN 系统设计时 THEN 系统 SHALL 实现 **`RocketMQSink`**（`SyncSink` 的默认实现），职责为：
   - 通过 ZMQ REQ Socket 从 Source 拉取 `SyncRecord`
   - 执行 Topic 过滤（白名单匹配）
   - **严格按照 `SyncRecord.physicOffset` 升序将消息写入目标 RocketMQ 集群**，保证 Source 获取到 Master 的消息顺序与 Sink 写入目标的顺序严格一致
   - 写入目标集群时，保留原始消息的 `queueId`，确保消息写入目标集群的同名 Topic 的**相同 Queue**，保证同一 Queue 内的消息顺序与源集群完全一致
   - 写入成功后调用 `CheckpointCoordinator.commitOffset()` 推进位点
   - 异常恢复时允许少量重复消息（At-Least-Once 语义），通过启动一致性校验优化重复范围

5. WHEN 系统设计时 THEN 系统 SHALL 确保 Source 和 Sink 的**监控指标相互独立**，并支持全链路 Trace 跟踪（详见需求 19）

6. WHEN 系统设计时 THEN 系统 SHALL 支持通过工厂方法或构造器注入不同的 `SyncSink` 实现，以便未来扩展，而无需修改 `HASource` 或 `SourceBootstrap` 的代码

**消息顺序严格一致性保证：**

6a. WHEN HASource 解析 CommitLog 数据包时 THEN 系统 SHALL 严格按照 CommitLog 中消息的物理偏移量（`physicOffset`）顺序产出 `SyncRecord`，不得乱序、跳跃或并行产出
6b. WHEN Source 通过 ZMQ 向 Sink 推送数据时 THEN `PullResponse` 中的 `records[]` 数组 SHALL 严格按照 `physicOffset` 升序排列
6c. WHEN Sink 从 Source 拉取到一批 `SyncRecord` 后 THEN Sink SHALL **严格按照 `physicOffset` 升序逐条写入目标集群**，当前消息写入成功并确认后才能写入下一条；禁止对同一批次内的消息进行并行写入或乱序写入
6d. WHEN Sink 将消息写入目标集群时 THEN 系统 SHALL 通过 `MessageQueueSelector` 将消息路由到与源集群**相同的 `queueId`**，确保同一 Topic 的同一 Queue 内消息顺序与源集群完全一致
6e. WHEN Topic 在目标集群的 Queue 配置与源集群完全一致时（由需求 12 保证）THEN 系统 SHALL 保证：对于源集群中 Topic T 的 Queue Q 上的消息序列 [M1, M2, M3, ...]，目标集群中 Topic T 的 Queue Q 上的消息顺序也为 [M1, M2, M3, ...]
6f. WHEN Sink 写入消息失败需要重试时 THEN 系统 SHALL 在原消息重试成功之前，**阻塞**后续消息的写入，确保不因重试而导致消息乱序

**Source 与 Sink 独立部署（ZeroMQ 通信 + NameServer KV 服务发现）：**

7. WHEN Source 启动时 THEN 系统 SHALL 在指定端口（`--zmqBindPort`，默认 5555）启动 ZeroMQ REP Socket，等待 Sink 的 Pull 请求；Source 将解析后的 `SyncRecord` 暂存于本地内存缓冲区，Sink 通过 ZMQ REQ-REP 模式主动拉取
8. WHEN Source 启动成功后 THEN 系统 SHALL 将自身 ZMQ 地址注册到**源集群 NameServer** 的 KV 存储中（namespace: `SYNC_SOURCE_CONFIG`，key: `{sourceNodeId}`，value: `{host}:{zmqPort}:{timestamp}`），并定期刷新（默认每 30 秒）；每个 Source 使用自身的 `sourceNodeId` 作为唯一 key，支持多个 Source 同时存在，Sink 通过遍历 `SYNC_SOURCE_CONFIG` namespace 下的所有 key 发现所有可用 Source 地址
9. WHEN Sink 启动时 THEN 系统 SHALL 通过源集群 NameServer 的 KV 接口（`GET_KV_LIST_BY_NAMESPACE`，namespace: `SYNC_SOURCE_CONFIG`）查询所有 Source 的 ZMQ 地址列表，并选择可用的 Source 通过 ZMQ REQ Socket 连接拉取数据
10. WHEN Sink 拉取数据时 THEN 系统 SHALL 通过 ZMQ REQ 发送 `PullRequest`（包含 `topicFilter`、`fromOffset`、`batchSize`），Source 通过 ZMQ REP 返回 `PullResponse`（包含 `records[]`、`maxOffset`）
11. WHEN Sink 成功将数据写入目标集群后 THEN 系统 SHALL 将自身的 `commitOffset` 写入**目标集群 NameServer** 的 KV 存储中（namespace: `SYNC_CHECKPOINT`，key: `{brokerName}:sink:{sinkId}:commitOffset`），Source 定期读取所有 Sink 的 commitOffset，取 `min` 值作为 `globalCheckpoint` 并写入 KV（key: `{brokerName}:globalCheckpoint`）
12. WHEN Source 或 Sink 重启时 THEN 系统 SHALL 从目标集群 NameServer KV 中读取 `globalCheckpoint`（Source）或自身的 `commitOffset`（Sink）恢复状态，实现**完全无状态**设计
13. WHEN Source 优雅关闭时 THEN 系统 SHALL 从源集群 NameServer KV 中删除自身的注册信息（`DELETE_KV_CONFIG`，key: `{sourceNodeId}`）

---

### 需求 3：不参与选举约束

**用户故事：** 作为一名数据同步组件，我希望 HASource 以只读 Learner 模式接入 Master，不影响集群的主从切换决策，以便安全地复制数据而不干扰生产集群。

#### 验收标准

1. WHEN 组件向 Master 发送握手或偏移量上报时 THEN 系统 SHALL 不向 NameServer 注册为 Broker，不发送 Broker 心跳
2. WHEN 组件连接 Master 时 THEN 系统 SHALL 仅使用 DefaultHAService 的基础 Slave 协议（`slaveMaxOffset` 上报），不发送 AutoSwitchHAService 的 HANDSHAKE 包（不携带 slaveAddress 等选举相关字段）
3. WHEN Master 进行 SyncStateSet 计算时 THEN 系统 SHALL 不被纳入 SyncStateSet，即 Master 不会因为本组件的同步进度而阻塞同步写入
4. WHEN 组件运行时 THEN 系统 SHALL 不向 Controller 发送任何选举相关请求

---

## 阶段二：Source 核心

### 需求 4：动态发现 Master 地址
**用户故事：** 作为一名数据同步组件，我希望 HASource 通过 NameServer 查询集群信息，获取当前 Master Broker 的 HA 服务地址，以便建立复制连接。

#### 验收标准

1. WHEN 组件启动时 THEN 系统 SHALL 解析命令行参数中的源集群 NameServer 地址（格式：`--sourceNamesrv <host:port>`，支持多个地址以 `;` 分隔）
2. WHEN 组件启动时 THEN 系统 SHALL 通过 NameServer 的 `GET_BROKER_CLUSTER_INFO` 接口查询集群中所有 Broker 信息（`ClusterInfo`）
3. WHEN 从 ClusterInfo 中解析 Broker 列表时 THEN 系统 SHALL 识别 brokerId=0 的节点为 Master，并获取其 HA 服务地址（haServerAddr）
4. IF NameServer 返回多个 Broker 组时 THEN 系统 SHALL 默认选取第一个 Broker 组的 Master
5. IF NameServer 连接失败 THEN 系统 SHALL 按指数退避策略重试（初始 1s，最大 30s），并记录错误日志

---

### 需求 5：启动兼容性预校验

**用户故事：** 作为一名运维人员，我希望在启动时先做消息解析尝试，如果解析失败则认为不兼容，任务直接失败退出，以便在不兼容的情况下明确告警，避免静默失败。

#### 验收标准

1. WHEN 组件启动并成功连接到 Master 后 THEN 系统 SHALL 在正式开始同步前执行兼容性预校验：
   - 从 Master 拉取少量 CommitLog 数据（至少包含 1 条完整消息，最多拉取 1MB）
   - 尝试解析拉取到的消息，校验 magicCode、totalSize、bodyCRC 等字段
2. WHEN 兼容性预校验中消息解析成功时 THEN 系统 SHALL 打印 INFO 日志（"兼容性预校验成功，消息格式兼容，准备开始同步"），并继续正式同步流程
3. WHEN 兼容性预校验中消息解析失败时 THEN 系统 SHALL 打印 **ERROR** 日志（包含失败原因，如 magicCode 不匹配的具体值），并**立即以非零退出码退出进程**（不等待人工确认），提示"消息格式不兼容，任务失败"
4. WHEN 兼容性预校验中 Master 没有任何 CommitLog 数据（`maxPhyOffset == 0`）时 THEN 系统 SHALL 跳过兼容性预校验，打印 WARN 日志（"Master CommitLog 为空，跳过兼容性预校验"），并继续正式同步流程
5. WHEN 兼容性预校验失败导致进程退出时 THEN 系统 SHALL 在退出前打印完整的错误信息，包含：Master 地址、拉取的偏移量范围、解析失败的具体字段和期望值/实际值对比

---

### 需求 6：CommitLog 过期检测

**用户故事：** 作为一名数据同步组件，我希望在启动时校验 Checkpoint 中的偏移量是否仍在 Master 可用范围内，以便在 CommitLog 已被清理的情况下明确告警，避免静默丢数据。

#### 验收标准

1. WHEN 组件启动并连接到 Master 后 THEN 系统 SHALL 向 Master 查询当前 CommitLog 的最小可用偏移量（`minPhyOffset`）和最大偏移量（`maxPhyOffset`）
2. WHEN 查询到 Master 的 `minPhyOffset` 后 THEN 系统 SHALL 将 Checkpoint 中的 `confirmedOffset` 与 `minPhyOffset` 进行比较：
   - IF `confirmedOffset` >= `minPhyOffset` THEN 系统 SHALL 正常从 `confirmedOffset` 开始同步
   - IF `confirmedOffset` < `minPhyOffset` THEN 系统 SHALL 打印 **ERROR** 日志，提示"Checkpoint offset 已过期，Master 最小可用偏移量为 X，当前 Checkpoint 为 Y，存在数据丢失风险"，并**阻塞启动**，等待人工确认
3. WHEN 系统因 offset 过期而阻塞启动时 THEN 系统 SHALL 支持通过环境变量 `HA_SOURCE_RESET_TO_EARLIEST=true` 强制从 Master 最早可用偏移量重新同步（需业务方显式确认，不可默认开启，仅供紧急场景使用）
4. WHEN 环境变量 `HA_SOURCE_RESET_TO_EARLIEST=true` 生效时 THEN 系统 SHALL 打印 WARN 日志，明确提示"将从 earliest offset 重新同步，可能产生重复数据"，并更新 Checkpoint 的 `confirmedOffset` 为 `minPhyOffset`
5. WHEN 组件运行期间检测到 `confirmedOffset` 落后于 `minPhyOffset`（即 Master 在运行中清理了已同步的 CommitLog）时 THEN 系统 SHALL 打印 ERROR 日志并触发告警，提示数据丢失风险，但不中断当前同步

---

### 需求 7：消息完整性与顺序

**用户故事：** 作为一名数据同步组件，我希望对接收到的每条消息进行完整性校验，并在输出中保留物理偏移量作为绝对顺序依据，以便保证数据的正确性和可追溯性。

#### 验收标准

**消息完整性校验：**

1. WHEN 解析 CommitLog 数据包中的消息时 THEN 系统 SHALL 校验以下完整性条件，任一失败则判定为解析失败：
   - **magicCode 校验**：消息头中的 magicCode 必须为 `MESSAGE_MAGIC_CODE`（0xAABBCCDD）或 `MESSAGE_MAGIC_CODE_V2`，否则判定为版本不兼容
   - **totalSize 校验**：消息头中的 `totalSize` 字段值必须大于消息头最小长度（20 字节），且不超过单条消息最大长度（默认 4MB）
   - **实际读取长度校验**：实际从缓冲区读取的字节数必须等于 `totalSize`，否则判定为消息截断（半包）
   - **bodyCRC 校验**：对消息 body 计算 CRC32，与消息头中的 `bodyCRC` 字段比对
2. WHEN 消息完整性校验失败时 THEN 系统 SHALL 按需求 15（DLQ）的消息解析失败处理流程处理（记录日志、封装 DLQ、跳过继续）
3. WHEN 启动时 THEN 系统 SHALL 执行兼容性预校验（详见需求 5），若 magicCode 不兼容则直接失败退出

**顺序性保证：**

4. WHEN HASource 将消息封装为 `SyncRecord` 时 THEN 系统 SHALL 将消息在 CommitLog 中的物理偏移量（`physicOffset`，从消息头解析）填入 `SyncRecord.physicOffset` 字段，作为消息的绝对顺序依据
5. WHEN Sink 将消息写入目标 RocketMQ 时 THEN 系统 SHALL 在消息的 Properties 中保留 `ORIGIN_PHYSICAL_OFFSET` 属性，值为 `SyncRecord.physicOffset` 的字符串表示，以便消费方按物理偏移量排序
6. WHEN 同一数据包内的消息写入目标 RocketMQ 时 THEN 系统 SHALL 保证消息的写入顺序与其在 CommitLog 中的物理偏移量顺序一致（即 `physicOffset` 小的消息先写入）

---

### 需求 8：Master 切换重连

**用户故事：** 作为一名数据同步组件，我希望在 Master 宕机、Slave 被提升为新 Master 或 Master 重启后，能自动发现并连接新的 Master，并从最新 Checkpoint 断点续传，以便数据同步不中断。

#### 验收标准

**动态发现新 Master：**

1. WHEN 与 Master 的 TCP 连接断开时 THEN 系统 SHALL 立即触发重连流程，重新向 NameServer 查询 brokerId=0 的节点 HA 地址
2. WHEN 重新查询到的 Master HA 地址与之前不同时 THEN 系统 SHALL 记录 Master 切换日志（包含旧地址、新地址、切换时间），并连接新的 Master 地址，`masterSwitchCount` 计数器加 1
3. WHEN 重连失败时 THEN 系统 SHALL 按指数退避策略重试（初始 1s，每次翻倍，最大 30s），并累加 `retryCount` 计数器
4. WHEN 重连成功后 THEN 系统 SHALL 从 Checkpoint 管理器读取最新的已确认位点（`confirmedOffset`），以该值作为新连接的起始 `slaveMaxOffset` 上报，从断点处继续同步
5. WHEN 组件运行期间 THEN 系统 SHALL 启动一个后台定时任务（默认每 30 秒），主动轮询 NameServer 检测 Master 是否发生变更，若变更则主动触发重连

**Master 地址列表配置（可选）：**

---

## 阶段三：Checkpoint + 最终一致性

### 需求 9：Checkpoint 断点续传

**用户故事：** 作为一名数据同步组件，我希望 `CheckpointCoordinator` 将同步进度（CommitLog 物理偏移量）持久化到本地 Checkpoint 文件，以便在进程重启、网络中断或 Master 切换后能从上次中断的位置继续同步，避免数据重复或丢失。`HASource` 读取位点用于上报，`Sink` 写入位点用于推进。

#### 验收标准

1. WHEN 组件首次启动时 THEN 系统 SHALL 在工作目录下创建 Checkpoint 文件（`checkpoint.json`），记录以下字段：
   - `confirmedOffset`：当前已确认同步的最大物理偏移量，初始值为 0
   - `lastFlushTime`：最近一次刷写时间（ISO-8601 格式）
   - `version`：文件格式版本号，当前为 `1`
2. WHEN 成功将一个数据包**写入本地存储并 fsync 落盘**后 THEN 系统 SHALL 更新内存中的 `confirmedOffset` 为该数据包的结束偏移量（`masterPhyOffset + bodySize`）；在落盘完成之前，`confirmedOffset` 不得推进
3. WHEN `confirmedOffset` 更新后 THEN 系统 SHALL 异步将最新位点刷写到 Checkpoint 文件，触发条件为以下任一先到者：
   - 距上次刷写超过 `checkpointFlushInterval`（默认 1000ms）
   - 累计新增数据包数量达到 `checkpointFlushBatchSize`（默认 100 个）
4. WHEN 刷写 Checkpoint 文件时 THEN 系统 SHALL 采用"先写临时文件再原子重命名"的方式，防止写入过程中崩溃导致文件损坏
5. WHEN 组件重启时 THEN 系统 SHALL 优先从 `checkpoint.json` 读取 `confirmedOffset`，并以该值作为初始 `slaveMaxOffset` 上报给 Master，从断点处继续拉取数据
6. IF Checkpoint 文件读取失败、文件不存在或 JSON 解析异常 THEN 系统 SHALL 记录 WARN 日志，并将 `confirmedOffset` 重置为 0，从头开始同步
7. WHEN 组件优雅关闭时 THEN 系统 SHALL 在退出前强制执行一次 Checkpoint 文件的同步刷写（fsync），确保最新位点落盘
8. WHEN Master 发生切换后重连新 Master 时 THEN 系统 SHALL 读取 Checkpoint 文件中的 `confirmedOffset`，以该值作为新连接的起始偏移量上报，实现跨 Master 切换的断点续传
9. IF Checkpoint 文件所在目录不存在 THEN 系统 SHALL 自动递归创建该目录，并记录 INFO 日志

---

### 需求 10：最终一致性保证

**用户故事：** 作为一名数据同步组件，我希望在掉电、进程突然宕机等极端场景下，系统能保证**最终一致性**——即所有消息最终都会被成功同步到目标集群，允许在异常恢复过程中出现少量重复消息（At-Least-Once），但不丢失任何消息。同时在每次启动时，通过对 Checkpoint 位点之后连续 X 条消息的存在性校验，快速确认数据的连续性和一致性。

#### 验收标准

**数据不丢失保证（At-Least-Once）：**

1. WHEN Sink 写入数据到目标集群时 THEN 系统 SHALL 遵循"先写数据到目标集群、确认写入成功、最后推进 Checkpoint"的严格顺序，确保 Checkpoint 中记录的 `confirmedOffset` 仅代表已成功写入目标集群的数据
2. WHEN 进程在写入目标集群**期间**（尚未收到写入确认）发生宕机时 THEN 系统 SHALL 在重启后从 Checkpoint 的 `confirmedOffset` 重新拉取该数据包并重新写入，不丢失该数据包；目标集群可能产生少量重复消息，属于预期行为
3. WHEN 进程在数据**已写入目标集群但 Checkpoint 尚未刷写**时发生宕机 THEN 系统 SHALL 在重启后从 Checkpoint 的旧 `confirmedOffset` 重新拉取并重新写入，目标集群中可能已存在该数据的副本，产生重复消息，属于预期行为
4. WHEN 进程在 Checkpoint **已刷写完成后**发生宕机 THEN 系统 SHALL 在重启后从新的 `confirmedOffset` 继续拉取，不重复同步已确认的数据
5. WHEN 向 Master 上报 `slaveMaxOffset` 时 THEN 系统 SHALL 上报 `confirmedOffset`（已确认写入目标集群的位点），而非内存中尚未确认的接收位点，确保 Master 不会因为本组件的虚假进度而误判同步状态

**启动一致性校验（Checkpoint 后连续消息存在性检查）：**

6. WHEN 启动参数中指定了 `--startupCheckMsgCount <X>`（可选，默认 10）时 THEN 系统 SHALL 在 Sink 启动、从 Checkpoint 恢复位点后，执行启动一致性校验：从 `confirmedOffset` 位点之后读取连续 X 条消息，逐一检查这些消息是否已存在于目标集群中
7. WHEN 执行启动一致性校验时 THEN 系统 SHALL 通过消息的 `ORIGIN_PHYSICAL_OFFSET` 属性（写入目标集群时保留的源集群物理偏移量）在目标集群中进行查询匹配，判断消息是否已存在
8. WHEN 连续 X 条消息**全部存在**于目标集群时 THEN 系统 SHALL 打印 INFO 日志（"启动一致性校验通过，Checkpoint 之后 X 条消息均已存在于目标集群，数据连续性确认"），跳过这 X 条消息，从第 X+1 条消息的偏移量开始同步，避免重复写入
9. WHEN 连续 X 条消息中**存在缺失**时 THEN 系统 SHALL 打印 WARN 日志（"启动一致性校验发现缺失：第 N 条消息不存在于目标集群，将从 Checkpoint 位点重新同步"），从 Checkpoint 的 `confirmedOffset` 位点开始重新拉取和写入，允许产生少量重复消息以确保不丢失
10. WHEN `--startupCheckMsgCount` 设置为 0 时 THEN 系统 SHALL 跳过启动一致性校验，直接从 Checkpoint 的 `confirmedOffset` 开始同步
11. WHEN Checkpoint 的 `confirmedOffset` 为 0（首次启动）时 THEN 系统 SHALL 跳过启动一致性校验，直接从头开始同步

---

## 阶段四：Sink 核心

### 需求 11：Topic 过滤同步

**用户故事：** 作为一名数据同步组件，我希望 Sink 支持按 Topic 白名单过滤 `SyncRecord`，仅将指定 Topic 的消息写入目标 RocketMQ 集群，以便减少不必要的数据写入并聚焦于目标数据。Topic 过滤在 Sink 侧执行，HASource 不感知过滤逻辑。

#### 验收标准

1. WHEN Sink 的配置文件或环境变量中指定了 `topicFilter` 时 THEN 系统 SHALL 将其解析为 Topic 白名单集合（大小写敏感，多个 Topic 以 `,` 分隔），仅对白名单内的 Topic 消息执行写入操作；该白名单将通过 `PullRequest.topicFilter` 传递给 Source
2. WHEN Sink 配置中未指定 `topicFilter` 时 THEN 系统 SHALL 对所有 Topic 的消息执行写入操作（即不过滤）
3. WHEN Sink 从队列中取出一条 `SyncRecord` 时 THEN Sink SHALL 读取 `SyncRecord.topic` 字段，与白名单进行匹配
4. IF 消息的 Topic 不在白名单中 THEN 系统 SHALL 跳过该消息的写入，并将 `filteredMessageCount` 计数器加 1
5. IF 消息的 Topic 在白名单中 THEN 系统 SHALL 将该消息写入目标 RocketMQ 集群，并将 `syncSuccessCount` 计数器加 1
6. WHEN HASource 解析 CommitLog 数据包中的消息时 THEN HASource SHALL 按照 RocketMQ MessageDecoder 的格式解析消息头（包含 totalSize、magicCode、topic 等字段），将 topic 字段填入 `SyncRecord`；若解析失败则记录错误并跳过该消息，不产出 `SyncRecord`
7. WHEN Topic 过滤开启时 THEN 系统 SHALL 无论消息是否通过过滤，均正常推进 `confirmedOffset`（即 Checkpoint 位点以数据包为粒度，而非消息为粒度），确保不因过滤而导致位点停滞
8. WHEN 组件运行时 THEN 系统 SHALL 支持通过 `GET /metrics` 接口查看当前生效的 Topic 白名单列表（`activeTopicFilter` 字段）

---

### 需求 12：全量元数据同步

**用户故事：** 作为一名数据同步组件，我希望在同步 CommitLog 消息数据的同时，也将源集群的全量元数据同步到目标 RocketMQ 集群，以便目标集群能正常提供服务，包含 Topic 配置、消费者位点、延迟消息位点、订阅组配置、消息请求模式和定时消息指标。

#### 验收标准

**元数据同步范围（对应 `SlaveSynchronize.syncAll()` 的全部方法）：**

1. WHEN 组件启动时 THEN 系统 SHALL 向源集群 Master 拉取以下全量元数据，并写入目标 RocketMQ 集群（通过目标集群的 Admin API）：
   - **Topic 配置**（`syncTopicConfig`）：通过 `GET_ALL_TOPIC_CONFIG` 接口获取 `TopicConfigAndMappingSerializeWrapper`，包含所有 Topic 的 `TopicConfig`（读写队列数、权限、属性等）和 `TopicQueueMappingDetail`（逻辑队列映射关系）
   - **消费者位点**（`syncConsumerOffset`）：通过 `GET_ALL_CONSUMER_OFFSET` 接口获取 `ConsumerOffsetSerializeWrapper`，包含所有 ConsumerGroup 在各 Topic 各 Queue 上的消费位点（`offsetTable: Map<String, Map<Integer, Long>>`）
   - **延迟消息位点**（`syncDelayOffset`）：通过 `GET_ALL_DELAY_OFFSET` 接口获取延迟消息各级别的消费位点字符串，写入目标集群的 `delayOffset.json` 文件
   - **订阅组配置**（`syncSubscriptionGroupConfig`）：通过 `GET_ALL_SUBSCRIPTIONGROUP_CONFIG` 接口获取 `SubscriptionGroupWrapper`，包含所有 ConsumerGroup 的订阅配置（重试队列数、消费模式、Broker ID 等）
   - **消息请求模式**（`syncMessageRequestMode`）：通过 `GET_ALL_MESSAGE_REQUEST_MODE` 接口获取 `MessageRequestModeSerializeWrapper`，包含各 Topic 各 ConsumerGroup 的消息拉取模式（PULL/POP）
   - **定时消息指标**（`syncTimerMetrics`，仅当 `timerWheelEnable=true` 时）：通过 `GET_TIMER_METRICS` 接口获取 `TimerMetrics.TimerMetricsSerializeWrapper`，包含各定时级别的消息计数（`timingCount: Map<Long, TimerMetrics.Metric>`）

2. WHEN 元数据同步时 THEN 系统 SHALL 采用**增量同步**策略：通过比较源集群与目标集群的 `DataVersion`，仅在版本不一致时执行同步，避免不必要的写入
3. WHEN 元数据同步时 THEN 系统 SHALL 对目标集群执行**全量覆盖**（先删除目标集群中源集群不存在的条目，再更新/新增），与 `SlaveSynchronize` 的行为保持一致
4. WHEN 组件启动时 THEN 系统 SHALL 在 CommitLog 数据同步开始前先完成一次全量元数据同步，确保目标集群的元数据与源集群一致
5. WHEN 组件运行时 THEN 系统 SHALL 启动一个后台定时任务（默认每 60 秒，可通过 `--metaSyncInterval <ms>` 配置），定期执行元数据同步
6. WHEN 元数据同步失败时 THEN 系统 SHALL 记录 ERROR 日志（包含失败的元数据类型、错误原因），并将 `metaSyncErrorCount` 计数器加 1，但不中断 CommitLog 数据同步
7. WHEN 目标集群中不存在某个 Topic 时 THEN 系统 SHALL 在目标集群自动创建该 Topic（通过 `UPDATE_AND_CREATE_TOPIC` 接口），Topic 配置与源集群保持一致
8. WHEN 组件启动时 THEN 系统 SHALL 使用 `--targetNamesrv` 地址连接目标 RocketMQ 集群进行元数据同步

**写入时 Topic 按需同步（实时拦截）：**

9. WHEN Sink 准备向目标集群写入一条消息时 THEN 系统 SHALL 首先检查该消息的 Topic 是否已存在于目标集群（通过本地缓存的 Topic 列表判断）；IF Topic 已存在 THEN 直接写入；IF Topic 不存在 THEN 触发按需 Topic 同步流程
10. WHEN 触发按需 Topic 同步时 THEN 系统 SHALL 从源集群 Master 拉取该 Topic 的完整 `TopicConfig`（包含读写队列数、权限、topicSysFlag、属性等），并通过目标集群的 `UPDATE_AND_CREATE_TOPIC` 接口在目标集群创建该 Topic，确保源集群与目标集群的 Topic 配置**完全一致**
11. WHEN 按需 Topic 同步**成功**时 THEN 系统 SHALL 将该 Topic 加入本地缓存的已同步 Topic 列表，打印 INFO 日志（"Topic [{topicName}] 在目标集群不存在，已从源集群同步创建成功"），并继续执行消息写入
12. WHEN 按需 Topic 同步**失败**时（源集群查询失败、目标集群创建失败等）THEN 系统 SHALL 按指数退避策略重试（初始间隔 500ms，最大间隔 5s，最多重试 `--topicSyncMaxRetry` 次，默认 3 次）
13. WHEN 按需 Topic 同步重试均失败后 THEN 系统 SHALL 执行以下操作：
    - 将同步状态置为 `TOPIC_SYNC_SUSPENDED`，**暂停整个 Sink 的消息写入流程**（不仅是该 Topic 的消息）
    - 打印 ERROR 日志（"Topic [{topicName}] 同步到目标集群失败，同步已暂停，原因：{errorReason}"）
    - 将 `topicSyncFailureCount` 监控计数器加 1
    - 上报监控告警（通过 `/metrics` 接口暴露 `topicSyncSuspended=true` 和 `topicSyncFailedTopic={topicName}`）
14. WHEN 同步状态为 `TOPIC_SYNC_SUSPENDED` 时 THEN 系统 SHALL 每隔 30 秒自动重试一次失败的 Topic 同步；IF 重试成功 THEN 系统 SHALL 将状态恢复为 `RUNNING`，打印 INFO 日志，继续正常写入
15. WHEN 同步状态为 `TOPIC_SYNC_SUSPENDED` 时 THEN 系统 SHALL 支持通过 `POST /resume` HTTP 接口手动触发恢复（强制重试 Topic 同步）
16. WHEN Sink 启动时 THEN 系统 SHALL 初始化本地 Topic 缓存：从目标集群的 NameServer 查询当前已存在的全部 Topic 列表，加载到内存缓存中，避免每条消息都查询远程
17. WHEN 后台定时元数据同步（验收标准第 5 条）成功同步 Topic 配置后 THEN 系统 SHALL 同步更新本地 Topic 缓存，确保缓存与目标集群状态一致

**元数据同步监控指标（新增至 Pipeline 侧指标）：**

18. WHEN 组件运行时 THEN 系统 SHALL 在 Pipeline 侧指标中新增以下元数据同步指标：
   - **metaSyncSuccessCount**：元数据同步成功的累计次数（按类型分别统计）
   - **metaSyncErrorCount**：元数据同步失败的累计次数
   - **lastMetaSyncTime**：最近一次元数据同步成功的时间戳（ISO-8601）
   - **topicSyncOnDemandCount**：按需 Topic 同步触发的累计次数
   - **topicSyncFailureCount**：按需 Topic 同步失败的累计次数
   - **topicSyncSuspended**：当前是否因 Topic 同步失败而暂停（boolean）
   - **topicSyncFailedTopic**：导致暂停的 Topic 名称（暂停时有值，运行正常时为空）

---

### 需求 13：解析失败 RFQ

**用户故事：** 作为一名数据同步组件，我希望将解析失败的消息原始字节写入目标 RocketMQ 集群的专用 Topic，以便后续能对这些异常数据进行重放、分析和修复，不丢失任何原始数据。

#### 验收标准

**RFQ 配置：**

1. WHEN 系统启动时 THEN 系统 SHALL 始终启用 RFQ 功能，将解析失败的消息发送到**源集群**（`--sourceNamesrv` 指定的集群）的 RFQ Topic，无需额外开关控制
2. WHEN 启动参数中指定了 `--rfqTopic` 时 THEN 系统 SHALL 使用该值作为 RFQ Topic 名称；若未指定则默认为 `ha-sync-rfq`
3. WHEN 启动参数中指定了 `--rfqProducerGroup` 时 THEN 系统 SHALL 使用该值作为 RFQ Producer 的 Group 名称；若未指定则默认为 `ha-sync-rfq-producer`

**ReplicaFailRecord 数据结构：**

5. WHEN HASource 解析消息失败时 THEN 系统 SHALL 将失败消息封装为 `ReplicaFailRecord`，包含以下字段：
   - `rawBytes`：消息的原始字节数组（从数据包中截取的完整消息字节）
   - `masterPhyOffset`：该消息所在数据包的起始物理偏移量
   - `offsetInPacket`：该消息在数据包内的相对偏移量
   - `errorReason`：解析失败的错误描述（如 `INVALID_MAGIC_CODE`、`INVALID_BODY_SIZE` 等枚举值）
   - `failTimestamp`：解析失败的时间戳（ISO-8601）
   - `sourceCluster`：来源集群的 NameServer 地址（即本组件的 `--sourceNamesrv` 参数值）

**RFQ 写入行为：**

6. WHEN RFQ 功能开启时 THEN 系统 SHALL 实现 `RfqSink` 组件，负责将 `ReplicaFailRecord` 序列化为 JSON 并作为 RocketMQ 消息的 Body 发送到**源集群**（`--sourceNamesrv` 指定的集群）的 RFQ Topic
7. WHEN `RfqSink` 向源集群发送消息时 THEN 系统 SHALL 在消息的 Properties 中设置以下属性，以便消费方快速过滤和路由：
   - `MASTER_PHY_OFFSET`：对应的 `masterPhyOffset` 字符串表示
   - `ERROR_REASON`：解析失败的错误类型
   - `SOURCE_CLUSTER`：来源集群 NameServer 地址
   - `FAIL_TIMESTAMP`：失败时间戳
8. WHEN `RfqSink` 向源集群发送消息时 THEN 系统 SHALL 使用同步发送模式（`send` 而非 `sendOneway`），确保消息已被源集群 Broker 确认接收
9. WHEN `RfqSink` 发送消息失败时 THEN 系统 SHALL 按指数退避策略重试（最多 `--rfqMaxRetry` 次，默认 3 次，初始间隔 500ms）；IF 重试均失败 THEN 系统 SHALL 将该 `ReplicaFailRecord` 写入本地备用文件（`./rfq-fallback.jsonl`，追加写入）并记录 ERROR 日志，确保原始数据不丢失
10. WHEN `RfqSink` 初始化时 THEN 系统 SHALL 复用 HASource 已建立的源集群连接（`--sourceNamesrv`）创建 RocketMQ Producer；IF 连接失败 THEN 系统 SHALL 记录 ERROR 日志，但不阻止主同步流程启动，RFQ 消息将暂时写入本地备用文件

**RFQ 监控指标（新增至 Sink 侧指标）：**

11. WHEN 组件运行时 THEN 系统 SHALL 在 Sink 侧指标中新增以下 RFQ 监控指标：
    - **rfqSendSuccessCount**：成功发送到源集群 RFQ Topic 的消息条数
    - **rfqSendFailureCount**：发送到源集群 RFQ Topic 失败（含重试均失败）的消息条数
    - **rfqFallbackCount**：因发送失败而写入本地备用文件的 RFQ 消息条数

---

## 阶段五：可靠性增强

### 需求 14：异常处理策略

**用户故事：** 作为一名数据同步组件，我希望在各类异常场景下能自动恢复并继续同步，以便在生产环境中保持高可用性。异常处理主要由 HASource 负责连接层恢复，Source 和 Sink 各自负责自身组件的异常处理和重启，恢复后通过 Checkpoint 断点续传保证最终一致性（允许少量重复消息）。

#### 验收标准

**Master 宕机处理：**

1. WHEN 与 Master 的 TCP 连接因 Master 宕机而断开时 THEN 系统 SHALL 立即触发重连流程，向 NameServer 查询新的 Master HA 地址
2. WHEN 重连 Master 失败时 THEN 系统 SHALL 按指数退避策略等待后重试（初始等待 1s，每次翻倍，最大等待 30s），并将 `retryCount` 计数器加 1
3. WHEN 连续重连失败超过 10 分钟时 THEN 系统 SHALL 打印 ERROR 级别日志并触发告警，但不退出进程，继续尝试重连

**网络闪断处理：**

4. WHEN TCP 连接因网络闪断（IOException、SocketException）而断开时 THEN 系统 SHALL 记录 WARN 日志，并从 Checkpoint 的 `confirmedOffset` 处重新建立连接，继续拉取数据（即从 last confirmed offset 重试）
5. WHEN 重连后向 Master 上报 `slaveMaxOffset` 时 THEN 系统 SHALL 使用 Checkpoint 中持久化的 `confirmedOffset`，而非内存中可能已失效的值，确保重试从正确位点开始
6. WHEN 网络闪断导致数据包接收不完整（半包）时 THEN 系统 SHALL 丢弃该不完整数据包，断开连接并触发重连，从上一个完整数据包的结束偏移量重新拉取

**消息解析失败处理：**

7. WHEN 解析 CommitLog 数据包中的某条消息时发生格式错误（magicCode 不匹配、长度字段异常等）THEN 系统 SHALL 记录 ERROR 日志（包含数据包的 `masterPhyOffset`、消息在包内的偏移量、错误原因），并将该条消息的**原始字节**封装为 `ReplicaFailRecord` 写入 RFQ（始终启用），同时跳过该条消息继续解析同一数据包中的后续消息
8. WHEN 单个数据包内解析失败的消息数量超过该数据包总消息数的 50% 时 THEN 系统 SHALL 记录 WARN 级别告警日志，提示数据包可能存在严重损坏，并将 `syncFailureCount` 计数器加 1
9. WHEN 消息解析失败时 THEN 系统 SHALL 将 `parseErrorCount` 计数器加 1，并在 `/metrics` 接口中暴露该指标
10. WHEN 消息解析失败被跳过后 THEN 系统 SHALL 仍正常推进 `confirmedOffset`（以数据包为粒度），不因单条消息解析失败而阻塞整体同步进度
11. IF 在 60 秒内 `parseErrorCount` 新增超过 100 次 THEN 系统 SHALL 打印 WARN 级别告警日志，提示消息格式异常频繁，建议人工介入排查

**解析失败暂停同步：**

12. WHEN `--parseErrorSuspendWindowMs`（默认 60000ms）滑动窗口内，`parseErrorCount` 新增次数超过 100 次 THEN 系统 SHALL 暂停 HASource 的数据拉取，将同步状态置为 `PARSE_ERROR_SUSPENDED`，并打印 ERROR 日志告警（暂停阈值可通过环境变量 `HA_SOURCE_PARSE_ERROR_SUSPEND_THRESHOLD` 调整，设为 0 则禁用暂停功能）
13. WHEN 同步状态为 `PARSE_ERROR_SUSPENDED` 时 THEN 系统 SHALL 停止向 Master 拉取新数据，但保持 TCP 连接不断开；Pipeline 队列中已有的 `SyncRecord` 继续被 Sink 消费处理
14. WHEN 同步状态为 `PARSE_ERROR_SUSPENDED` 时 THEN 系统 SHALL 每隔 30 秒打印一次 ERROR 日志，提示当前处于暂停状态、暂停原因、已暂停时长，直到人工干预恢复
15. WHEN 同步状态为 `PARSE_ERROR_SUSPENDED` 时 THEN 系统 SHALL 通过 `POST /resume` HTTP 接口支持人工触发恢复，恢复后重置滑动窗口计数，将状态置回 `RUNNING`，并打印 INFO 日志
16. WHEN 环境变量 `HA_SOURCE_PARSE_ERROR_SUSPEND_THRESHOLD` 设为 0 时 THEN 系统 SHALL 不启用暂停功能，解析失败仅记录日志并跳过，同步不中断

---

### 需求 15：网络抖动自动重试

**用户故事：** 作为一名运维人员，我希望在临时网络抖动、丢包等情况产生的抖动场景下，组件能自动重试，以便在生产环境中保持高可用性，无需人工干预。

#### 验收标准

**Sink 写入自动重试：**

1. WHEN Sink 向目标 RocketMQ 发送消息失败时（`RemotingException`、`MQBrokerException`、`InterruptedException` 等可重试异常）THEN 系统 SHALL 按指数退避策略自动重试（初始间隔 100ms，每次翻倍，最大间隔 5s，最大重试次数可通过 `--sinkMaxRetry` 配置，默认 3 次）
2. WHEN Sink 重试时 THEN 系统 SHALL 将 `retryCount` 计数器加 1，并在日志中记录重试次数和等待时间（DEBUG 级别）
3. WHEN Sink 重试次数达到上限仍失败时 THEN 系统 SHALL 将该消息记录为 `syncFailureCount`，打印 ERROR 日志，并将该消息写入 RFQ
4. WHEN Sink 向目标 RocketMQ 发送消息时遇到不可重试异常（如消息体超过最大限制、Topic 不存在且创建失败等）THEN 系统 SHALL 不重试，直接记录 ERROR 日志并将消息写入 RFQ

**NameServer 查询自动重试：**

5. WHEN HASource 向 NameServer 查询 Master 地址失败时 THEN 系统 SHALL 按指数退避策略自动重试（初始间隔 1s，最大间隔 30s），并将 `nameSrvQueryErrorCount` 计数器加 1

**元数据同步自动重试：**

6. WHEN 元数据同步失败时 THEN 系统 SHALL 在下一个定时周期自动重试，不需要人工干预

---

### 需求 16：目标不可写监控

**用户故事：** 作为一名运维人员，我希望在目标 RocketMQ 集群不可写时能及时感知并告警，同时组件能定期自动重试，以便在目标集群恢复后自动恢复同步，无需人工干预。

#### 验收标准

**目标不可写检测：**

1. WHEN Sink 向目标 RocketMQ 发送消息时连续失败超过 10 次 THEN 系统 SHALL 将目标集群状态标记为 `UNAVAILABLE`，并打印 ERROR 日志告警
2. WHEN 目标集群状态为 `UNAVAILABLE` 时 THEN 系统 SHALL 暂停向目标集群发送消息，避免无效重试消耗资源；同时 HASource 继续拉取数据并缓存在 Pipeline 队列中（队列满时阻塞 Source 拉取）
3. WHEN 目标集群状态为 `UNAVAILABLE` 时 THEN 系统 SHALL 启动定期探活任务（默认每 30 秒，可通过 `--targetProbeInterval <ms>` 配置），向目标集群发送探活消息（发送到专用探活 Topic `ha-sync-probe`）
4. WHEN 探活成功时 THEN 系统 SHALL 将目标集群状态恢复为 `AVAILABLE`，打印 INFO 日志，并恢复正常的消息发送流程

**目标不可写监控指标（新增至 Sink 侧指标）：**

5. WHEN 组件运行时 THEN 系统 SHALL 在 Sink 侧指标中新增以下目标集群状态指标：
   - **targetClusterStatus**：目标集群当前状态，枚举值为 `AVAILABLE` / `UNAVAILABLE`
   - **targetUnavailableDurationSeconds**：目标集群当前连续不可写的持续时长（秒），恢复后重置为 0
   - **targetProbeSuccessCount**：探活成功的累计次数
   - **targetProbeFailureCount**：探活失败的累计次数

6. WHEN 以下告警条件触发时 THEN 系统 SHALL 打印对应级别的告警日志：
   - IF `targetClusterStatus` 变为 `UNAVAILABLE` THEN 立即打印 **ERROR** 日志，提示目标集群不可写
   - IF `targetUnavailableDurationSeconds` 超过 300 秒（5 分钟）THEN 打印 **ERROR** 日志，提示目标集群长时间不可写，建议人工介入

---

### 需求 17：优雅停机快照

**用户故事：** 作为一名运维人员，我希望组件在收到停机信号时能优雅地完成当前批次处理并持久化位点，同时支持定期状态快照以加速故障恢复。

#### 验收标准

**优雅停机：**

1. WHEN 组件收到 SIGTERM 或 SIGINT 信号时 THEN 系统 SHALL 按以下顺序执行优雅停机：
   - 停止 HASource 拉取新数据（关闭 TCP 连接）
   - 等待 Pipeline 队列中已有的 `SyncRecord` 全部被 Sink 处理完成（最长等待 30 秒）
   - 强制执行一次 Checkpoint 文件的同步刷写（fsync）
   - 停止所有后台线程，关闭 HTTP 监控服务
   - 以退出码 0 退出进程
2. WHEN 优雅停机等待超过 30 秒时 THEN 系统 SHALL 强制停止所有线程，执行最后一次 Checkpoint 刷写后退出，并打印 WARN 日志提示强制退出

**定期状态快照：**

3. WHEN 组件运行时 THEN 系统 SHALL 定期（默认每 60 秒）将当前运行状态写入快照文件（`./snapshot.json`），包含以下字段：
   - `confirmedOffset`：当前已确认落盘的同步位点
   - `masterAddr`：当前连接的 Master 地址
   - `snapshotTime`：快照时间（ISO-8601）
   - `topicBytesStats`：各 Topic 累计同步字节数
   - `syncSuccessCount`、`syncFailureCount`、`parseErrorCount` 等关键计数器
4. WHEN 组件重启时 THEN 系统 SHALL 优先读取快照文件中的状态，与 Checkpoint 文件合并（以 `confirmedOffset` 较大者为准），加速故障恢复
5. WHEN 写入快照文件时 THEN 系统 SHALL 采用"先写临时文件再原子重命名"的方式，防止写入过程中崩溃导致文件损坏

---

## 阶段六：分布式 & 高性能

### 需求 18：Source 多实例注册与 Sink 分布式消费

**用户故事：** 作为一名数据同步组件，我希望 Source 是无状态的，可以存在多个实例，每个 Source 将自身信息注册到源集群 NameServer 的独立唯一 KV 中；Source 故障后由外部工具自动拉起。Sink 是分布式的，Source 将流量分发给已注册的 Sink 列表，以便实现高吞吐的数据同步。

#### 验收标准

**Source 无状态多实例：**

1. WHEN Source 节点启动时 THEN 系统 SHALL 将自身信息注册到**源集群 NameServer** 的 KV 存储中（namespace: `SYNC_SOURCE_CONFIG`，key: `{sourceNodeId}`，value: `{host}:{zmqPort}:{timestamp}`），`sourceNodeId` 是该 Source 实例的唯一标识（可通过 `--sourceNodeId` 参数指定，默认为 `hostname:pid`）
2. WHEN 多个 Source 实例同时运行时 THEN 系统 SHALL 允许每个 Source 独立注册自己的 KV，互不冲突；每个 Source 使用自身唯一的 `sourceNodeId` 作为 key
3. WHEN Source 节点故障时 THEN 系统 SHALL 由外部运维工具（如 K8s、Supervisor 等）负责自动拉起，新实例从 Checkpoint 的 `confirmedOffset` 断点续传
4. WHEN Source 节点启动时 THEN 系统 SHALL 在启动日志中打印当前节点标识（`sourceNodeId`）
5. WHEN Source 优雅关闭时 THEN 系统 SHALL 从源集群 NameServer KV 中删除自身的注册信息（`DELETE_KV_CONFIG`，key: `{sourceNodeId}`）

**Source 流量统计：**

6. WHEN HASource 解析消息时 THEN 系统 SHALL 统计每个 Topic 的消息字节数（`topicBytesStats: Map<String, Long>`），并将该统计信息附加到 `SyncRecord` 或通过独立的统计快照定期发布
7. WHEN `topicBytesStats` 更新时 THEN 系统 SHALL 每隔 10 秒将各 Topic 的字节数统计打印到日志（INFO 级别），并通过 `/metrics` 接口暴露

**Sink 分布式消费（Source 分发模式）：**

8. WHEN Sink 节点启动时 THEN 系统 SHALL 向 Source 注册自身，Source 维护已注册的 Sink 列表
9. WHEN Source 收到多个 Sink 的 PullRequest 时 THEN 系统 SHALL 将数据按请求分发给已注册的 Sink 列表，不做负载均衡计算
10. WHEN Sink 节点数量发生变化（新增或下线）时 THEN 系统 SHALL 自动更新 Sink 列表，无需重启 Source

---

### 需求 19：全链路 Trace 监控

**用户故事：** 作为一名运维人员，我希望从 Source 解析到 Sink 写入的全部处理过程都有 Trace 跟踪和监控指标，并且保证高的同步 TPS，以便在出现问题时能快速定位，同时满足生产环境的吞吐量要求。

#### 验收标准

**全链路 Trace 跟踪：**

1. WHEN HASource 解析消息时 THEN 系统 SHALL 为每条消息生成唯一的 Trace ID（格式：`{nodeId}-{masterPhyOffset}-{offsetInPacket}`），并填入 `SyncRecord.traceId` 字段
2. WHEN Sink 将消息写入目标 RocketMQ 时 THEN 系统 SHALL 在消息的 Properties 中保留 `SYNC_TRACE_ID` 属性，值为 `SyncRecord.traceId`，以便消费方追踪消息来源
3. WHEN 消息在 Source 侧被解析时 THEN 系统 SHALL 记录 Trace 事件（`SOURCE_PARSED`），包含：`traceId`、`masterPhyOffset`、`topic`、`msgSize`、`parseTimestamp`
4. WHEN 消息在 Sink 侧被成功写入目标 RocketMQ 时 THEN 系统 SHALL 记录 Trace 事件（`SINK_WRITTEN`），包含：`traceId`、`targetMsgId`（目标集群返回的 msgId）、`writeTimestamp`、`latencyMs`（从 Source 解析到 Sink 写入的端到端延迟）
5. WHEN 消息在任意环节处理失败时 THEN 系统 SHALL 记录 Trace 事件（`FAILED`），包含：`traceId`、`failStage`（`SOURCE` 或 `SINK`）、`errorReason`、`failTimestamp`
6. WHEN 组件运行时 THEN 系统 SHALL 在 Pipeline 侧指标中新增以下 Trace 相关指标：
   - **avgEndToEndLatencyMs**：消息从 Source 解析到 Sink 写入的平均端到端延迟（毫秒，滑动窗口 1 分钟）
   - **p99EndToEndLatencyMs**：P99 端到端延迟（毫秒，滑动窗口 1 分钟）
   - **currentTps**：当前每秒处理的消息条数（滑动窗口 1 秒）

**高 TPS 保障：**

7. WHEN 系统设计时 THEN 系统 SHALL 采用以下机制保证高同步 TPS：
   - HASource 使用 NIO 非阻塞 IO 接收数据，避免 IO 阻塞影响吞吐量
   - Source 将解析后的 SyncRecord 暂存于本地内存缓冲区，Sink 通过 ZMQ REQ-REP 模式主动拉取，批次大小可通过 `--sinkBatchSize` 配置（默认 100）
   - Sink 支持批量发送（`--sinkBatchSize`，默认 100 条/批），减少 RocketMQ Producer 的 RPC 调用次数
   - Sink 支持多线程并发写入（`--sinkThreads`，默认 4 个线程），充分利用多核 CPU
8. WHEN Source 内存缓冲区中待拉取的 SyncRecord 数量持续超过 80% 上限时 THEN 系统 SHALL 打印 WARN 日志，提示 Sink 处理速度跟不上 Source 拉取速度，建议增加 Sink 线程数或 Sink 节点数

---

## 阶段七：可观测性

### 需求 20：监控指标采集

**用户故事：** 作为一名运维人员，我希望能实时查看数据同步组件的运行状态、关键指标以及各类异常情况，以便全面监控同步健康状况并快速定位问题。指标按 Source 侧、Sink 侧、Pipeline 侧三个维度分组采集，由 `MetricsCollector` 统一聚合暴露。

#### 验收标准

1. WHEN 组件运行时 THEN 系统 SHALL 统计并暴露以下 **Source 侧指标**（由 `HASource` 采集）：
   - **connectionStatus**：当前连接状态，枚举值为 `CONNECTED` / `RECONNECTING` / `DISCONNECTED` / `PARSE_ERROR_SUSPENDED`
   - **currentMasterAddr**：当前连接的 Master HA 地址（`host:port`）
   - **continuousFailDurationSeconds**：当前连续重连失败的持续时长（秒），连接成功后重置为 0
   - **connectionErrorCount**：TCP 连接断开的累计次数（含 Master 宕机、网络闪断、连接超时）
   - **retryCount**：累计重连 Master 的次数（含指数退避重试）
   - **nameSrvQueryErrorCount**：向 NameServer 查询 Master 地址失败的累计次数
   - **parseErrorCount**：消息解析失败（magicCode 不匹配、长度字段异常等）并跳过的消息条数
   - **halfPacketDropCount**：因接收到不完整数据包（半包）而主动断开并丢弃的累计次数
   - **offsetMismatchCount**：接收到的 `masterPhyOffset` 与本地期望偏移量不一致的累计次数
   - **masterSwitchCount**：检测到 Master 地址发生变更（主从切换）的累计次数
   - **parseErrorSuspendStatus**：当前是否处于解析失败暂停状态，枚举值为 `RUNNING` / `PARSE_ERROR_SUSPENDED`
   - **parseErrorSuspendDurationSeconds**：当前连续处于暂停状态的持续时长（秒），恢复后重置为 0
   - **parseErrorSuspendCount**：历史上因解析失败触发暂停的累计次数

2. WHEN 组件运行时 THEN 系统 SHALL 统计并暴露以下 **Sink 侧指标**（由 `RocketMQSink` 采集）：
   - **syncSuccessCount**：累计成功写入目标集群的消息条数
   - **syncFailureCount**：累计写入失败的消息条数（目标集群写入失败）
   - **filteredMessageCount**：因 Topic 过滤而跳过写入的消息条数
   - **storageWriteErrorCount**：目标集群写入失败的累计次数
   - **checkpointFlushErrorCount**：Checkpoint 刷写失败的累计次数
   - **startupCheckResult**：最近一次启动一致性校验结果，枚举值为 `PASSED` / `FAILED` / `SKIPPED`
   - **startupCheckMsgFound**：启动一致性校验中在目标集群找到的消息条数

3. WHEN 组件运行时 THEN 系统 SHALL 统计并暴露以下 **Pipeline 侧指标**（由 Source/Sink 协调聚合）：
   - **syncBytesPerSecond**：每秒从 Master 接收并写入的字节数（滑动窗口，窗口大小 1 秒）
   - **confirmedOffset**：当前已确认落盘的同步位点（来自 `CheckpointCoordinator`）
   - **masterOffset**：最近一次从 Master 收到的数据包的起始偏移量（来自 `HASource`）
   - **lagBytes**：`masterOffset - confirmedOffset`，表示当前同步滞后量（字节数）
   - **lastCheckpointFlushTime**：最近一次 Checkpoint 文件刷写成功的时间戳（ISO-8601）

4. WHEN 组件运行时 THEN 系统 SHALL 每隔 10 秒将上述全部指标打印到日志（INFO 级别），格式为结构化 JSON，字段按 Source/Sink/Pipeline 三组排列

5. WHEN 组件运行时 THEN 系统 SHALL 在 Source 和 Sink 各自的 HTTP 监控端口（Source 默认 `--sourceMetricsPort` 9876，Sink 默认 `--sinkMetricsPort` 9877）提供 `GET /metrics` 接口，返回当前全部指标的 JSON 快照，Sink 侧同时包含 `activeTopicFilter` 字段（当前生效的 Topic 白名单列表）

6. WHEN 以下任一告警条件触发时 THEN 系统 SHALL 打印对应级别的告警日志：
   - IF `syncFailureCount` 在 60 秒内新增超过 10 次 THEN 打印 **WARN** 日志，提示写入异常频繁
   - IF `parseErrorCount` 在 60 秒内新增超过 100 次 THEN 打印 **WARN** 日志，提示消息格式异常频繁，建议人工介入排查
   - IF `continuousFailDurationSeconds` 超过 600 秒（10 分钟）THEN 打印 **ERROR** 日志，提示长时间无法连接 Master
   - IF `lagBytes` 持续 60 秒超过 100MB THEN 打印 **WARN** 日志，提示同步严重滞后
   - IF `checkpointFlushErrorCount` 在 60 秒内新增超过 3 次 THEN 打印 **ERROR** 日志，提示 Checkpoint 持久化异常，存在数据重复风险
   - IF `parseErrorSuspendStatus` 为 `PARSE_ERROR_SUSPENDED` THEN 每隔 30 秒打印 **ERROR** 日志，提示同步已暂停、暂停原因及已暂停时长，直到人工通过 `POST /resume` 恢复

---


