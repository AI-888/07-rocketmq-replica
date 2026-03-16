# RocketMQ HA 数据同步组件 - 设计文档

## 1. 系统概述

### 1.1 设计目标

RocketMQ HA 数据同步组件是一个独立的 Java 程序，旨在模拟 RocketMQ Slave Broker 的主从复制行为，实现跨集群数据同步。组件采用 Source/Sink 分离架构，通过 RocketMQ 原生 HA 协议从源集群 Master 拉取 CommitLog 数据，并写入目标集群。

### 1.2 核心特性

- **Source/Sink 解耦**：Source 专注数据拉取与解析，Sink 专注数据写入，可独立扩展
- **独立部署**：Source 和 Sink 作为独立进程，通过 ZeroMQ 通信
- **完全无状态**：所有状态存储在目标集群 NameServer KV 中
- **消息顺序一致**：严格按照源集群物理偏移量顺序写入
- **最终一致性**：At-Least-Once 语义 + 启动一致性校验
- **高可用**：支持 Master 切换、断点续传、自动重连
- **可观测**：全链路 Trace + 丰富监控指标

### 1.3 适用场景

- 跨机房数据同步
- 灾备集群数据复制
- 数据迁移
- 多活架构数据同步

---

## 2. 系统架构

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            Sync Pipeline                                │
│                                                                          │
│  ┌──────────────────┐                           ┌──────────────────────┐ │
│  │   HASource       │    SyncRecord Queue       │  Sink (分布式多节点)  │ │
│  │  (单节点拉取)    │ ────────────────────────►  │  (写入目标 RocketMQ) │ │
│  │                  │   含流量统计元信息         │                      │ │
│  └──────────────────┘                           └──────────────────────┘ │
│         │                                                 │               │
│  CheckpointCoordinator (位点协调器) ◄───────────────────┘               │
└─────────────────────────────────────────────────────────────────────────┘

                    ┌───────────────────────────┐
                    │  源集群 NameServer         │
                    │  (发现 Master HA 地址)     │
                    └───────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────────┐
                    │  源集群 Master Broker      │
                    │  (HA 协议，TCP 连接)       │
                    └───────────────────────────┘

                    ┌───────────────────────────┐
                    │  目标集群 NameServer KV    │
                    │  - Source 地址注册         │
                    │  - Checkpoint 存储         │
                    └───────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────────┐
                    │  目标集群 RocketMQ         │
                    │  (消息写入 + RFQ)          │
                    └───────────────────────────┘
```

### 2.2 Source/Sink 独立部署架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Source Worker                            │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐   │
│  │ HA Connection│──►│ CommitLog    │──►│ ZeroMQ REP       │   │
│  │              │   │ Parser       │   │ Socket (5555)    │   │
│  └──────────────┘   └──────────────┘   └──────────────────┘   │
│         │                                        │              │
│         └─────► NameServer KV 注册地址 ◄─────────┘              │
└─────────────────────────────────────────────────────────────────┘

                              ▲
                              │ ZMQ REQ-REP
                              ▼

┌─────────────────────────────────────────────────────────────────┐
│                        Sink Worker 1                            │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐   │
│  │ ZeroMQ REQ   │──►│ Message      │──►│ Target RocketMQ  │   │
│  │ Client       │   │ Writer       │   │ Producer         │   │
│  └──────────────┘   └──────────────┘   └──────────────────┘   │
│         │                                        │              │
│         └─────► NameServer KV 更新 Checkpoint ◄──┘              │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        Sink Worker 2                            │
│  (多个 Sink 并行写入，Source 单节点保证顺序)                     │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 NameServer KV 数据模型

```
Namespace: SYNC_SOURCE_CONFIG
├── Key: {brokerName}
└── Value: {host}:{zmqPort}:{timestamp}  // Source ZMQ 地址

Namespace: SYNC_CHECKPOINT
├── Key: {brokerName}:globalCheckpoint
│   └── Value: {minOffset}  // 所有 Sink 的最小 commitOffset
├── Key: {brokerName}:sink:{sinkId}:commitOffset
│   └── Value: {offset}  // 各 Sink 的已提交位点
└── Key: {brokerName}:source:topicStats
    └── Value: {topic1:bytes,topic2:bytes,...}  // Topic 流量统计
```

---

## 3. 核心组件设计

### 3.1 HASource（数据源）

#### 3.1.1 职责

- 通过 NameServer 发现 Master HA 地址
- 使用 DefaultHAService Slave 协议建立 TCP 连接
- 持续接收 CommitLog 数据包并解析
- 统计各 Topic 流量（字节数）
- 将解析后的 `SyncRecord` 通过 ZMQ 发送给 Sink

#### 3.1.2 核心类设计

```java
public class HASource implements SyncSource {
    private final SyncConfig config;
    private final HASourceConnection haConnection;
    private final CommitLogParser parser;
    private final SourceRegistry registry;
    private final ZMQ.Socket zmqSocket;
    private final Map<String, AtomicLong> topicBytesStats;
    
    @Override
    public void start() {
        // 1. 发现 Master HA 地址
        // 2. 建立 TCP 连接
        // 3. 启动 ZMQ REP Socket
        // 4. 注册到 NameServer KV
        // 5. 启动后台定时任务（Master 轮询、统计刷新）
    }
    
    @Override
    public void poll() {
        // 1. 从 TCP 连接接收数据包
        // 2. 解析消息（CommitLogParser）
        // 3. 统计 Topic 流量
        // 4. 封装为 SyncRecord
        // 5. 响应 Sink 的 Pull 请求（ZMQ）
    }
    
    @Override
    public void stop() {
        // 1. 关闭 TCP 连接
        // 2. 关闭 ZMQ Socket
        // 3. 从 NameServer KV 删除注册
    }
}
```

#### 3.1.3 HA 协议交互流程

```
Source (Slave)                              Master
    │                                          │
    ├──────────────► TCP 连接 ────────────────►│
    │                                          │
    ├──────────────► 上报 slaveMaxOffset ─────►│
    │                (8 字节)                  │
    │                                          │
    │◄─────────────  数据包 ◄──────────────────┤
    │          [masterPhyOffset(8) + bodySize(4) + body]
    │                                          │
    ├──────────────► 上报新的 slaveMaxOffset ─►│
    │                (confirmedOffset)         │
    │                                          │
    │◄─────────────  下一个数据包 ◄─────────────┤
    │                                          │
    └────────────────────────────────────────►└──
```

### 3.2 RocketMQSink（数据写入）

#### 3.2.1 职责

- 通过 NameServer KV 发现 Source ZMQ 地址
- 通过 ZMQ REQ 拉取 `SyncRecord`
- 执行 Topic 过滤（白名单）
- 严格按 physicOffset 顺序写入目标集群
- 写入成功后更新 Checkpoint

#### 3.2.2 核心类设计

```java
public class RocketMQSink implements SyncSink {
    private final SyncConfig config;
    private final DefaultMQProducer producer;
    private final ZMQ.Socket zmqSocket;
    private final CheckpointCoordinator checkpointCoordinator;
    private final Set<String> topicFilter;
    
    @Override
    public void start() {
        // 1. 从 NameServer KV 查询 Source 地址
        // 2. 连接 Source 的 ZMQ REP Socket
        // 3. 初始化 RocketMQ Producer
        // 4. 从 Checkpoint 恢复位点
    }
    
    @Override
    public void write(SyncRecord record) {
        // 1. Topic 过滤（白名单匹配）
        // 2. 选择目标 Queue（保持源 queueId）
        // 3. 同步发送消息到目标集群
        // 4. 写入成功后调用 checkpointCoordinator.commitOffset()
    }
    
    @Override
    public void flush() {
        // 批量发送时的刷写逻辑
    }
    
    @Override
    public void stop() {
        // 1. 等待队列中的消息全部写入
        // 2. 刷写最后一次 Checkpoint
        // 3. 关闭 Producer
        // 4. 关闭 ZMQ Socket
    }
}
```

#### 3.2.3 消息顺序保证机制

```java
// 1. Source 按 physicOffset 顺序产出 SyncRecord
// 2. Sink 严格按接收顺序写入（单线程或分区锁）
// 3. 使用 MessageQueueSelector 保持 queueId 映射

public class FixedQueueSelector implements MessageQueueSelector {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        int queueId = (Integer) arg;  // 原始 queueId
        return mqs.stream()
            .filter(mq -> mq.getQueueId() == queueId)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Queue not found: " + queueId));
    }
}

// 写入时
SendResult result = producer.send(
    message, 
    new FixedQueueSelector(), 
    record.getQueueId()  // 保持原 queueId
);
```

### 3.3 CheckpointCoordinator（位点协调器）

#### 3.3.1 职责

- 管理全局同步位点（`globalCheckpoint`）
- 协调多个 Sink 的 `commitOffset`
- 持久化 Checkpoint 到 NameServer KV
- 提供断点续传恢复

#### 3.3.2 核心类设计

```java
public class CheckpointCoordinator {
    private final MQClientAPIImpl mqClientAPI;
    private final String brokerName;
    private volatile long globalCheckpoint;
    private final Map<String, Long> sinkCommitOffsets;
    
    /**
     * Sink 写入成功后调用，更新该 Sink 的 commitOffset
     */
    public void commitOffset(String sinkId, long offset) {
        sinkCommitOffsets.put(sinkId, offset);
        
        // 异步刷写到 NameServer KV
        scheduleFlush();
    }
    
    /**
     * Source 定期调用，计算 globalCheckpoint（所有 Sink 的最小值）
     */
    public long getGlobalCheckpoint() {
        long minOffset = sinkCommitOffsets.values().stream()
            .min(Long::compare)
            .orElse(0L);
        
        globalCheckpoint = minOffset;
        
        // 刷写 globalCheckpoint 到 NameServer KV
        persistGlobalCheckpoint(globalCheckpoint);
        
        return globalCheckpoint;
    }
    
    /**
     * 启动时从 NameServer KV 恢复位点
     */
    public long recoverCheckpoint(String sinkId) {
        String key = brokerName + ":sink:" + sinkId + ":commitOffset";
        String value = mqClientAPI.getKVConfig(
            "SYNC_CHECKPOINT", 
            key
        );
        return value != null ? Long.parseLong(value) : 0L;
    }
}
```

#### 3.3.3 Checkpoint 刷写策略

```java
// 触发条件（任一满足即触发）：
// 1. 累计新增数据包达到 checkpointFlushBatchSize（默认 100）
// 2. 距上次刷写超过 checkpointFlushInterval（默认 1000ms）
// 3. 优雅停机时强制刷写

private void scheduleFlush() {
    if (shouldFlush()) {
        executorService.submit(() -> {
            try {
                // 批量写入所有 Sink 的 commitOffset
                for (Map.Entry<String, Long> entry : sinkCommitOffsets.entrySet()) {
                    String key = brokerName + ":sink:" + entry.getKey() + ":commitOffset";
                    mqClientAPI.putKVConfig(
                        "SYNC_CHECKPOINT", 
                        key, 
                        String.valueOf(entry.getValue())
                    );
                }
                lastFlushTime = System.currentTimeMillis();
            } catch (Exception e) {
                log.error("Checkpoint flush failed", e);
                metricsCollector.incrementCheckpointFlushError();
            }
        });
    }
}
```

### 3.4 CommitLogParser（消息解析器）

#### 3.4.1 消息格式

```
CommitLog 数据包格式：
┌────────────────────────────────────────────────┐
│  masterPhyOffset (8 bytes)                     │  ← 数据包起始偏移量
├────────────────────────────────────────────────┤
│  bodySize (4 bytes)                            │  ← body 长度
├────────────────────────────────────────────────┤
│  body (variable length)                        │
│    ├─ Message 1 (totalSize, magicCode, ...)   │
│    ├─ Message 2                                │
│    └─ Message N                                │
└────────────────────────────────────────────────┘

单条消息格式（参考 MessageDecoder）：
┌────────────────────────────────────────────────┐
│  totalSize (4 bytes)                           │
├────────────────────────────────────────────────┤
│  magicCode (4 bytes)  // 0xAABBCCDD / V2       │
├────────────────────────────────────────────────┤
│  bodyCRC (4 bytes)                             │
├────────────────────────────────────────────────┤
│  queueId (4 bytes)                             │
├────────────────────────────────────────────────┤
│  ...                                           │
│  topic (variable)                              │
│  body (variable)                               │
└────────────────────────────────────────────────┘
```

#### 3.4.2 解析流程

```java
public class CommitLogParser {
    
    public List<SyncRecord> parse(byte[] data, long masterPhyOffset) {
        List<SyncRecord> records = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        long currentOffset = masterPhyOffset;
        
        while (buffer.hasRemaining()) {
            // 1. 读取 totalSize
            int totalSize = buffer.getInt();
            
            // 2. 校验 magicCode
            int magicCode = buffer.getInt();
            if (!isValidMagicCode(magicCode)) {
                handleParseError(currentOffset, "Invalid magicCode: " + magicCode);
                continue;
            }
            
            // 3. 解析消息头
            int bodyCRC = buffer.getInt();
            int queueId = buffer.getInt();
            // ... 其他字段
            
            // 4. 读取 topic
            int topicLength = buffer.get();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);
            
            // 5. 读取 body
            int bodyLength = totalSize - HEADER_SIZE - topicLength;
            byte[] body = new byte[bodyLength];
            buffer.get(body);
            
            // 6. CRC 校验（可选）
            if (config.isEnableBodyCRC() && !validateCRC(body, bodyCRC)) {
                handleParseError(currentOffset, "CRC mismatch");
                continue;
            }
            
            // 7. 封装 SyncRecord
            SyncRecord record = new SyncRecord();
            record.setMasterPhyOffset(masterPhyOffset);
            record.setPhysicOffset(currentOffset);  // 绝对顺序依据
            record.setTopic(topic);
            record.setQueueId(queueId);
            record.setBody(body);
            record.setMsgSize(totalSize);
            record.setStoreTimestamp(System.currentTimeMillis());
            record.setTraceId(generateTraceId(currentOffset));
            
            records.add(record);
            
            // 统计 Topic 流量
            topicBytesStats.computeIfAbsent(topic, k -> new AtomicLong())
                .addAndGet(totalSize);
            
            currentOffset += totalSize;
        }
        
        return records;
    }
    
    private void handleParseError(long offset, String reason) {
        log.error("Parse error at offset {}: {}", offset, reason);
        metricsCollector.incrementParseError();
        
        // 封装为 ReplicaFailRecord 发送到 RFQ
        rfqSink.send(new ReplicaFailRecord(
            rawBytes, offset, reason, System.currentTimeMillis()
        ));
    }
}
```

### 3.5 MetadataSyncService（元数据同步）

#### 3.5.1 同步范围

| 元数据类型 | 源接口 | 目标接口 | 同步频率 |
|-----------|--------|---------|---------|
| Topic 配置 | `GET_ALL_TOPIC_CONFIG` | `UPDATE_AND_CREATE_TOPIC` | 启动 + 每 60s |
| 消费者位点 | `GET_ALL_CONSUMER_OFFSET` | `UPDATE_CONSUMER_OFFSET` | 每 60s |
| 延迟消息位点 | `GET_ALL_DELAY_OFFSET` | 文件写入 | 每 60s |
| 订阅组配置 | `GET_ALL_SUBSCRIPTIONGROUP_CONFIG` | `UPDATE_SUBSCRIPTIONGROUP_CONFIG` | 每 60s |
| 消息请求模式 | `GET_ALL_MESSAGE_REQUEST_MODE` | `UPDATE_MESSAGE_REQUEST_MODE` | 每 60s |

#### 3.5.2 Topic 按需同步

```java
public class TopicOnDemandSync {
    
    /**
     * Sink 写入前检查 Topic 是否存在，不存在则按需同步
     */
    public boolean ensureTopicExists(String topic) {
        // 1. 检查本地缓存
        if (localTopicCache.contains(topic)) {
            return true;
        }
        
        // 2. 从源集群查询 TopicConfig
        TopicConfig sourceConfig = adminExt.examineTopicConfig(
            sourceNameSrv, 
            topic
        );
        
        // 3. 在目标集群创建 Topic
        try {
            adminExt.createAndUpdateTopicConfig(
                targetNameSrv,
                sourceConfig
            );
            
            localTopicCache.add(topic);
            log.info("Topic [{}] synced to target cluster", topic);
            return true;
            
        } catch (Exception e) {
            log.error("Failed to sync topic [{}]", topic, e);
            
            // 重试逻辑
            return retryTopicSync(topic, sourceConfig);
        }
    }
    
    private boolean retryTopicSync(String topic, TopicConfig config) {
        int maxRetry = this.config.getTopicSyncMaxRetry();
        
        for (int i = 0; i < maxRetry; i++) {
            try {
                Thread.sleep(500 * (1 << i));  // 指数退避
                adminExt.createAndUpdateTopicConfig(targetNameSrv, config);
                return true;
            } catch (Exception e) {
                log.warn("Retry {}/{} failed for topic [{}]", i+1, maxRetry, topic);
            }
        }
        
        // 重试失败后暂停同步
        suspendSync(topic, "Topic sync failed after " + maxRetry + " retries");
        return false;
    }
}
```

---

## 4. 关键流程设计

### 4.1 启动流程

```
┌──────────────────────────────────────────────────────────────┐
│                      Source 启动流程                          │
└──────────────────────────────────────────────────────────────┘

1. 加载配置（环境变量 > CLI > 配置文件）
2. 解析 NameServer 地址（--namesrv）
3. 通过 NameServer 查询 Master HA 地址
   └─ GET_BROKER_CLUSTER_INFO → 选择 brokerId=0
4. 兼容性预校验
   ├─ 拉取 1MB CommitLog 数据
   ├─ 尝试解析消息（校验 magicCode）
   └─ 失败则退出，成功则继续
5. CommitLog 过期检测
   ├─ 查询 Master minPhyOffset
   ├─ 对比 Checkpoint confirmedOffset
   └─ 过期则等待人工确认或 --resetToEarliest
6. 建立 TCP 连接（Master HA 地址）
7. 启动 ZMQ REP Socket（绑定 --zmqBindPort）
8. 注册到目标集群 NameServer KV
   └─ Namespace: SYNC_SOURCE_CONFIG
       Key: {brokerName}
       Value: {host}:{zmqPort}:{timestamp}
9. 启动后台定时任务
   ├─ Master 地址轮询（30s）
   ├─ 统计刷新（10s）
   └─ Checkpoint 刷写（1s / 100 条）
10. 上报初始 slaveMaxOffset（从 globalCheckpoint 恢复）
11. 进入数据接收循环

┌──────────────────────────────────────────────────────────────┐
│                      Sink 启动流程                            │
└──────────────────────────────────────────────────────────────┘

1. 加载配置（环境变量 > CLI > 配置文件）
2. 从目标集群 NameServer KV 查询 Source 地址
   └─ Namespace: SYNC_SOURCE_CONFIG
       Key: {brokerName}
3. 连接 Source 的 ZMQ REP Socket
4. 初始化 RocketMQ Producer（目标集群）
5. 从 NameServer KV 恢复 Checkpoint
   └─ Key: {brokerName}:sink:{sinkId}:commitOffset
6. 启动一致性校验（可选）
   ├─ 从 Checkpoint 位点读取 X 条消息
   ├─ 检查目标集群是否已存在
   └─ 全部存在则跳过，否则从 Checkpoint 重新同步
7. 初始化本地 Topic 缓存
   └─ 查询目标集群已存在的 Topic 列表
8. 启动后台定时任务
   ├─ 元数据同步（60s）
   └─ 快照写入（60s）
9. 进入 Pull 循环（ZMQ REQ 拉取 SyncRecord）
```

### 4.2 数据同步流程

```
Source                                              Sink
  │                                                  │
  ├─► 接收 Master 数据包                             │
  │   [masterPhyOffset + bodySize + body]           │
  │                                                  │
  ├─► 解析 CommitLog                                 │
  │   ├─ 校验 magicCode                              │
  │   ├─ 解析消息头（topic, queueId, body）          │
  │   ├─ CRC 校验（可选）                            │
  │   ├─ 统计 Topic 流量                             │
  │   └─ 封装为 SyncRecord                           │
  │                                                  │
  ├─► 暂存 SyncRecord（内存缓冲区）                  │
  │                                                  │
  │                                                  │
  │                  ZMQ REQ-REP                     │
  │◄───────────────── Pull Request ◄─────────────────┤
  │   {fromOffset, topicFilter, batchSize}          │
  │                                                  │
  ├────────────────► Pull Response ────────────────►│
  │   {records[], maxOffset}                         │
  │                                                  │
  │                                                  ├─► Topic 过滤（白名单）
  │                                                  │
  │                                                  ├─► Topic 按需同步
  │                                                  │   └─ 不存在则从源集群同步
  │                                                  │
  │                                                  ├─► 严格按 physicOffset 顺序写入
  │                                                  │   └─ 使用 FixedQueueSelector
  │                                                  │
  │                                                  ├─► 写入成功后更新 commitOffset
  │                                                  │   └─ CheckpointCoordinator.commitOffset()
  │                                                  │
  │                                                  ├─► 异步刷写 Checkpoint 到 NameServer KV
  │                                                  │   Key: {brokerName}:sink:{sinkId}:commitOffset
  │                                                  │
  │                                                  └─► 下一轮 Pull Request
  │                                                  │
  ├─► 定期计算 globalCheckpoint                      │
  │   └─ min(所有 Sink 的 commitOffset)              │
  │                                                  │
  ├─► 上报 globalCheckpoint 给 Master                │
  │   └─ 8 字节 slaveMaxOffset                       │
  │                                                  │
  └────────────────────────────────────────────────►└──
```

### 4.3 Master 切换流程

```
1. Source 检测到 TCP 连接断开
   └─ IOException / SocketException

2. 触发重连流程
   ├─ 关闭旧连接
   ├─ 清理本地缓冲区
   └─ 重置接收状态

3. 向 NameServer 查询新的 Master HA 地址
   └─ GET_BROKER_CLUSTER_INFO → 选择 brokerId=0

4. 对比新旧 Master 地址
   ├─ 地址不同 → Master 切换
   │   ├─ masterSwitchCount++
   │   └─ 打印 Master 切换日志
   └─ 地址相同 → Master 重启
       └─ 打印 Master 重连日志

5. 从 CheckpointCoordinator 读取 globalCheckpoint

6. 建立新 TCP 连接（新 Master HA 地址）

7. 上报 globalCheckpoint 作为 slaveMaxOffset
   └─ 从断点处继续拉取

8. 重连失败处理
   ├─ 指数退避重试（1s, 2s, 4s, ..., 最大 30s）
   ├─ 超过 10 分钟仍失败 → ERROR 级别告警
   └─ 不退出进程，持续重试
```

### 4.4 异常处理流程

#### 4.4.1 消息解析失败

```
1. CommitLogParser 解析消息失败
   ├─ magicCode 不匹配
   ├─ totalSize 异常
   └─ CRC 校验失败

2. 记录 ERROR 日志
   └─ 包含 offset、错误原因

3. 封装为 ReplicaFailRecord
   ├─ rawBytes: 原始字节数组
   ├─ masterPhyOffset: 数据包起始偏移量
   ├─ errorReason: 错误枚举
   └─ failTimestamp: 失败时间

4. 发送到源集群 RFQ Topic（ha-sync-rfq）
   └─ RfqSink.send()

5. 跳过该消息，继续解析后续消息
   └─ 不阻塞整体同步进度

6. parseErrorCount++

7. 检查解析失败率
   └─ IF 60s 内 parseErrorCount > parseErrorSuspendThreshold
       ├─ 暂停 Source 数据拉取
       ├─ 状态置为 PARSE_ERROR_SUSPENDED
       └─ 等待人工 POST /resume 恢复
```

#### 4.4.2 目标集群不可写

```
1. Sink 写入目标集群失败
   └─ RemotingException / MQBrokerException

2. 自动重试（指数退避，最多 3 次）
   └─ 100ms, 200ms, 400ms

3. 重试均失败
   ├─ 写入 RFQ（消息丢失兜底）
   └─ 连续失败次数 targetFailureCount++

4. IF targetFailureCount > targetUnavailableThreshold（默认 10）
   ├─ 标记目标集群状态为 UNAVAILABLE
   ├─ 暂停消息发送
   └─ 启动定期探活（30s）

5. 探活成功
   ├─ 状态恢复为 AVAILABLE
   ├─ targetFailureCount 重置为 0
   └─ 恢复正常发送
```

### 4.5 优雅停机流程

```
1. 接收到 SIGTERM / SIGINT 信号

2. 停止 Source 拉取新数据
   └─ 关闭 TCP 连接

3. 等待 Pipeline 队列中的数据全部消费
   └─ 最长等待 30 秒

4. Sink 刷写最后一次 Checkpoint
   └─ 同步调用 CheckpointCoordinator.flush()

5. 写入快照文件（snapshot.json）
   ├─ confirmedOffset
   ├─ masterAddr
   ├─ topicBytesStats
   └─ 各类计数器

6. 停止所有后台线程
   ├─ Master 轮询任务
   ├─ 统计刷新任务
   ├─ 元数据同步任务
   └─ 探活任务

7. 关闭 ZMQ Socket

8. 从 NameServer KV 删除 Source 注册
   └─ DELETE_KV_CONFIG

9. 关闭 HTTP 监控服务

10. 以退出码 0 退出进程
```

---

## 5. 数据模型

### 5.1 SyncRecord（数据传输单元）

```java
public class SyncRecord {
    // 数据包起始偏移量
    private long masterPhyOffset;
    
    // 数据包结束偏移量
    private long endOffset;
    
    // 消息物理偏移量（绝对顺序依据）
    private long physicOffset;
    
    // 消息 Topic
    private String topic;
    
    // 队列 ID
    private int queueId;
    
    // 消息体
    private byte[] body;
    
    // 消息大小（字节）
    private int msgSize;
    
    // 存储时间戳
    private long storeTimestamp;
    
    // 接收时间戳
    private long receiveTimestamp;
    
    // 全链路追踪 ID
    private String traceId;
    
    // 消息属性（可选）
    private Map<String, String> properties;
}
```

### 5.2 ReplicaFailRecord（解析失败记录）

```java
public class ReplicaFailRecord {
    // 原始字节数组
    private byte[] rawBytes;
    
    // 数据包起始偏移量
    private long masterPhyOffset;
    
    // 消息在数据包内的相对偏移量
    private int offsetInPacket;
    
    // 错误原因枚举
    private ErrorReason errorReason;
    
    // 失败时间戳
    private long failTimestamp;
    
    // 来源集群 NameServer 地址
    private String sourceCluster;
}

public enum ErrorReason {
    INVALID_MAGIC_CODE,
    INVALID_TOTAL_SIZE,
    INVALID_BODY_SIZE,
    CRC_MISMATCH,
    TRUNCATED_MESSAGE,
    UNKNOWN
}
```

### 5.3 PullRequest/PullResponse（ZMQ 通信）

```java
// Sink 向 Source 发送的拉取请求
public class PullRequest {
    // Topic 过滤（可选）
    private Set<String> topicFilter;
    
    // 起始偏移量
    private long fromOffset;
    
    // 批量大小
    private int batchSize;
    
    // Sink ID
    private String sinkId;
}

// Source 返回的拉取响应
public class PullResponse {
    // SyncRecord 列表（按 physicOffset 升序）
    private List<SyncRecord> records;
    
    // 当前最大偏移量
    private long maxOffset;
    
    // 响应状态
    private ResponseStatus status;
}

public enum ResponseStatus {
    SUCCESS,        // 成功
    NO_NEW_MSG,     // 暂无新消息
    OFFSET_ILLEGAL, // 偏移量非法
    ERROR           // 内部错误
}
```

### 5.4 Checkpoint（位点数据）

```java
// checkpoint.json
{
  "confirmedOffset": 1234567890,   // 已确认落盘的最大偏移量
  "lastFlushTime": "2026-03-16T10:30:00Z",  // 最近刷写时间
  "version": 1                      // 文件格式版本
}

// snapshot.json（快照文件）
{
  "confirmedOffset": 1234567890,
  "masterAddr": "192.168.1.100:10912",
  "snapshotTime": "2026-03-16T10:30:00Z",
  "topicBytesStats": {
    "TopicA": 102400000,
    "TopicB": 51200000
  },
  "syncSuccessCount": 123456,
  "syncFailureCount": 12,
  "parseErrorCount": 5
}
```

---

## 6. 监控指标设计

### 6.1 Source 侧指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `connectionStatus` | Gauge | 连接状态：CONNECTED / RECONNECTING / DISCONNECTED / PARSE_ERROR_SUSPENDED |
| `currentMasterAddr` | Gauge | 当前 Master HA 地址 |
| `continuousFailDurationSeconds` | Gauge | 连续重连失败时长（秒） |
| `connectionErrorCount` | Counter | TCP 连接断开累计次数 |
| `retryCount` | Counter | 重连 Master 累计次数 |
| `nameSrvQueryErrorCount` | Counter | NameServer 查询失败次数 |
| `parseErrorCount` | Counter | 消息解析失败次数 |
| `halfPacketDropCount` | Counter | 半包丢弃次数 |
| `offsetMismatchCount` | Counter | 偏移量不一致次数 |
| `masterSwitchCount` | Counter | Master 切换次数 |
| `parseErrorSuspendStatus` | Gauge | 暂停状态：RUNNING / PARSE_ERROR_SUSPENDED |
| `parseErrorSuspendDurationSeconds` | Gauge | 暂停持续时长（秒） |

### 6.2 Sink 侧指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `syncSuccessCount` | Counter | 写入成功消息数 |
| `syncFailureCount` | Counter | 写入失败消息数 |
| `filteredMessageCount` | Counter | Topic 过滤跳过消息数 |
| `storageWriteErrorCount` | Counter | 目标集群写入失败次数 |
| `checkpointFlushErrorCount` | Counter | Checkpoint 刷写失败次数 |
| `startupCheckResult` | Gauge | 启动一致性校验结果：PASSED / FAILED / SKIPPED |
| `startupCheckMsgFound` | Counter | 启动校验找到的消息数 |
| `targetClusterStatus` | Gauge | 目标集群状态：AVAILABLE / UNAVAILABLE |
| `targetUnavailableDurationSeconds` | Gauge | 目标集群不可写持续时长（秒） |
| `targetProbeSuccessCount` | Counter | 探活成功次数 |
| `targetProbeFailureCount` | Counter | 探活失败次数 |
| `rfqSendSuccessCount` | Counter | RFQ 发送成功次数 |
| `rfqSendFailureCount` | Counter | RFQ 发送失败次数 |
| `rfqFallbackCount` | Counter | RFQ 本地备用文件写入次数 |
| `topicSyncOnDemandCount` | Counter | Topic 按需同步触发次数 |
| `topicSyncFailureCount` | Counter | Topic 同步失败次数 |
| `topicSyncSuspended` | Gauge | Topic 同步暂停状态（boolean） |
| `topicSyncFailedTopic` | Gauge | 导致暂停的 Topic 名称 |

### 6.3 Pipeline 侧指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `syncBytesPerSecond` | Gauge | 每秒同步字节数 |
| `queueSize` | Gauge | 内部队列当前积压数 |
| `confirmedOffset` | Gauge | 当前已确认位点 |
| `masterOffset` | Gauge | Master 最新偏移量 |
| `lagBytes` | Gauge | 同步滞后字节数 |
| `lastCheckpointFlushTime` | Gauge | 最近 Checkpoint 刷写时间 |
| `metaSyncSuccessCount` | Counter | 元数据同步成功次数 |
| `metaSyncErrorCount` | Counter | 元数据同步失败次数 |
| `lastMetaSyncTime` | Gauge | 最近元数据同步时间 |
| `avgEndToEndLatencyMs` | Gauge | 平均端到端延迟（毫秒） |
| `p99EndToEndLatencyMs` | Gauge | P99 端到端延迟（毫秒） |
| `currentTps` | Gauge | 当前 TPS（条/秒） |

### 6.4 HTTP 监控接口

```
GET /metrics
返回 JSON 格式的所有指标：
{
  "source": {
    "connectionStatus": "CONNECTED",
    "currentMasterAddr": "192.168.1.100:10912",
    "parseErrorCount": 5,
    ...
  },
  "sink": {
    "syncSuccessCount": 123456,
    "syncFailureCount": 12,
    "targetClusterStatus": "AVAILABLE",
    ...
  },
  "pipeline": {
    "confirmedOffset": 1234567890,
    "lagBytes": 102400,
    "currentTps": 5000,
    ...
  },
  "activeTopicFilter": ["TopicA", "TopicB"]
}

POST /resume
手动恢复暂停状态（PARSE_ERROR_SUSPENDED / TOPIC_SYNC_SUSPENDED）

GET /health
健康检查接口
```

---

## 7. 性能优化策略

### 7.1 Source 端优化

1. **NIO 非阻塞 IO**：使用 Java NIO Selector 接收 Master 数据
2. **零拷贝解析**：直接在接收缓冲区解析消息，减少内存拷贝
3. **批量统计**：每 10 秒批量更新 Topic 流量统计
4. **内存缓冲区**：维护 1000 条 SyncRecord 的内存缓冲，减少 ZMQ 往返

### 7.2 Sink 端优化

1. **批量发送**：默认 100 条/批，减少 RPC 调用
2. **多线程并发**：默认 4 个 Sink 线程，充分利用多核
3. **本地 Topic 缓存**：避免每条消息查询 NameServer
4. **异步 Checkpoint**：批量刷写（100 条或 1 秒），不阻塞写入

### 7.3 网络优化

1. **ZMQ 高水位线**：设置发送/接收高水位线（HWM），防止内存爆炸
2. **TCP Nagle 算法禁用**：`TCP_NODELAY=true`，降低延迟
3. **连接池复用**：Producer 连接复用，减少握手开销

### 7.4 预期性能指标

| 场景 | Source TPS | Sink TPS | 端到端延迟 |
|------|-----------|----------|-----------|
| 单 Sink | 5,000 条/秒 | 5,000 条/秒 | < 100ms |
| 4 Sink 并行 | 20,000 条/秒 | 20,000 条/秒 | < 200ms |
| 高负载 | 50,000 条/秒 | 50,000 条/秒 | < 500ms |

---

## 8. 安全性设计

### 8.1 SQL 注入防护

- **不适用**：本组件不涉及 SQL 查询

### 8.2 配置安全

- **敏感信息掩码**：启动日志中 NameServer 地址仅打印部分（`192.168.*.***:9876`）
- **配置文件权限**：建议 `ha-sync.properties` 权限设置为 `600`

### 8.3 网络安全

- **不参与选举**：不向 NameServer 注册为 Broker，不影响主从切换
- **只读复制**：仅使用 DefaultHAService 基础 Slave 协议，不发送选举包

### 8.4 数据完整性

- **CRC 校验**：可选开启消息 body CRC 校验（`--enableBodyCRC=true`）
- **位点严格推进**：仅在 Sink 确认写入后才推进 Checkpoint

---

## 9. 扩展性设计

### 9.1 自定义 Sink 实现

```java
// 通过工厂方法注入自定义 Sink
public interface SyncSinkFactory {
    SyncSink createSink(SyncConfig config);
}

// 例如：实现写入 Kafka 的 Sink
public class KafkaSink implements SyncSink {
    @Override
    public void write(SyncRecord record) {
        kafkaProducer.send(new ProducerRecord<>(
            record.getTopic(),
            record.getBody()
        ));
    }
}
```

### 9.2 插件化架构预留

```java
// 在 SyncPipeline 中支持多个 Sink 链式处理
public class SyncPipeline {
    private final List<SyncSink> sinks;
    
    public void addSink(SyncSink sink) {
        this.sinks.add(sink);
    }
    
    private void dispatch(SyncRecord record) {
        for (SyncSink sink : sinks) {
            sink.write(record);
        }
    }
}
```

---

## 10. 测试策略

### 10.1 单元测试

- **CommitLogParser**：测试各种消息格式的解析
- **CheckpointCoordinator**：测试位点持久化和恢复
- **MetadataSync**：测试元数据同步逻辑

### 10.2 集成测试

- **Source-Sink 端到端**：启动 Source 和 Sink，验证数据传输
- **Master 切换**：模拟 Master 宕机，验证自动重连
- **网络闪断**：模拟网络中断，验证断点续传

### 10.3 性能测试

- **高 TPS 压测**：模拟 50,000 条/秒写入，验证吞吐量
- **长时间运行**：7x24 小时运行，验证稳定性

### 10.4 故障演练

- **掉电恢复**：强制 kill -9，验证 Checkpoint 恢复
- **目标集群不可写**：停止目标 Broker，验证探活和恢复

---

## 11. 部署架构

### 11.1 推荐部署拓扑

```
┌────────────────────────────────────────────────────────────┐
│                       源集群                                │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   │
│  │ NameServer 1 │   │ NameServer 2 │   │ NameServer 3 │   │
│  └──────────────┘   └──────────────┘   └──────────────┘   │
│  ┌──────────────┐   ┌──────────────┐                      │
│  │ Master Broker│   │ Slave Broker │                      │
│  │ (brokerId=0) │   │ (brokerId=1) │                      │
│  └──────────────┘   └──────────────┘                      │
└────────────────────────────────────────────────────────────┘
                    │
                    │ HA 协议（TCP）
                    ▼
┌────────────────────────────────────────────────────────────┐
│                    Source Worker（单节点）                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ HASource                                             │  │
│  │  - HA Connection                                     │  │
│  │  - CommitLog Parser                                  │  │
│  │  - ZMQ REP Socket (0.0.0.0:5555)                     │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
                    │
                    │ ZMQ REQ-REP
                    ▼
┌────────────────────────────────────────────────────────────┐
│                    Sink Worker 1                           │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ RocketMQSink                                         │  │
│  │  - ZMQ REQ Client                                    │  │
│  │  - RocketMQ Producer (目标集群)                      │  │
│  │  - Checkpoint Coordinator                            │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                    Sink Worker 2                           │
│  (多个 Sink 并行，按 Topic 流量负载均衡)                    │
└────────────────────────────────────────────────────────────┘
                    │
                    │ RocketMQ 协议
                    ▼
┌────────────────────────────────────────────────────────────┐
│                       目标集群                              │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   │
│  │ NameServer 1 │   │ NameServer 2 │   │ NameServer 3 │   │
│  │ (KV 存储)    │   │ (KV 存储)    │   │ (KV 存储)    │   │
│  └──────────────┘   └──────────────┘   └──────────────┘   │
│  ┌──────────────┐   ┌──────────────┐                      │
│  │ Broker 1     │   │ Broker 2     │                      │
│  └──────────────┘   └──────────────┘                      │
└────────────────────────────────────────────────────────────┘
```

### 11.2 启动命令示例

```bash
# Source Worker 启动（单节点）
java -jar ha-sync.jar \
  --mode=source \
  --namesrv=192.168.1.100:9876;192.168.1.101:9876 \
  --targetNamesrv=192.168.2.100:9876 \
  --brokerName=broker-a \
  --dataDir=/data/ha-sync-source \
  --zmqBindPort=5555 \
  --metricsPort=9876

# Sink Worker 启动（多节点）
java -jar ha-sync.jar \
  --mode=sink \
  --targetNamesrv=192.168.2.100:9876 \
  --brokerName=broker-a \
  --dataDir=/data/ha-sync-sink-1 \
  --sinkId=sink-1 \
  --topics=TopicA,TopicB \
  --sinkThreads=4 \
  --metricsPort=9877
```

### 11.3 Docker 部署

```dockerfile
FROM openjdk:8-jre-alpine

WORKDIR /app

COPY ha-sync.jar /app/
COPY ha-sync.properties /app/

EXPOSE 5555 9876

ENTRYPOINT ["java", "-jar", "ha-sync.jar"]
CMD ["--configFile=/app/ha-sync.properties"]
```

---

## 12. 运维指南

### 12.1 日志规范

- **日志级别**：INFO（正常流程） / WARN（告警） / ERROR（严重错误）
- **日志格式**：`[时间] [级别] [线程] [类名] - 消息`
- **关键日志**：
  - Source 启动/停止
  - Master 切换
  - Checkpoint 刷写
  - 解析失败（ERROR）
  - 目标集群不可写（ERROR）

### 12.2 告警规则

| 告警项 | 条件 | 级别 |
|-------|------|------|
| Master 长时间不可用 | `continuousFailDurationSeconds > 600` | P0 |
| 同步严重滞后 | `lagBytes > 100MB` 持续 60s | P1 |
| 解析失败频繁 | `parseErrorCount` 60s 新增 > 100 | P1 |
| 目标集群不可写 | `targetClusterStatus == UNAVAILABLE` | P0 |
| Checkpoint 刷写失败 | `checkpointFlushErrorCount` 60s 新增 > 3 | P0 |
| 同步已暂停 | `parseErrorSuspendStatus == SUSPENDED` | P0 |

### 12.3 故障排查清单

1. **Source 无法连接 Master**
   - 检查 NameServer 地址是否正确
   - 检查 Master HA 端口是否开放（默认 10912）
   - 检查网络连通性

2. **Sink 无法发现 Source**
   - 检查目标集群 NameServer KV 中是否有 Source 注册
   - 检查 `SYNC_SOURCE_CONFIG` namespace

3. **同步滞后严重**
   - 检查 Sink 线程数是否足够（`--sinkThreads`）
   - 检查目标集群写入性能
   - 增加 Sink 节点数

4. **消息重复**
   - 正常现象（At-Least-Once 语义）
   - 检查启动一致性校验是否开启（`--startupCheckMsgCount`）

---

## 13. 未来规划

### 13.1 Phase 2 功能

- **加密传输**：支持 TLS/SSL 加密 HA 连接
- **压缩传输**：支持 GZIP 压缩 CommitLog 数据
- **多租户支持**：支持多个源集群同时同步

### 13.2 Phase 3 功能

- **Web 管理控制台**：可视化配置、监控、告警
- **动态配置热更新**：无需重启修改 Topic 过滤
- **增量元数据同步**：基于 DataVersion 的增量更新

---

## 附录

### A. 参考文档

- [RocketMQ 官方文档](https://rocketmq.apache.org/)
- [DefaultHAService 源码](../store/src/main/java/org/apache/rocketmq/store/ha/DefaultHAService.java)
- [MessageDecoder 源码](../common/src/main/java/org/apache/rocketmq/common/message/MessageDecoder.java)

### B. 术语表

| 术语 | 说明 |
|-----|------|
| HA | High Availability，高可用 |
| CommitLog | RocketMQ 存储的消息日志文件 |
| physicOffset | 消息在 CommitLog 中的物理偏移量 |
| Checkpoint | 同步位点快照 |
| RFQ | Replica Fail Queue，副本失败队列 |
| ZMQ | ZeroMQ，高性能消息队列库 |
| KV | Key-Value，NameServer 的 KV 配置存储 |

---

**文档版本**：v1.0  
**最后更新**：2026-03-16  
**作者**：HA Sync Team
