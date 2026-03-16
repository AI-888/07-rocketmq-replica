# 需求文档：RocketMQ HA 数据同步 — 一致性校验

## 引言

本文档定义了 RocketMQ HA 数据同步组件的**数据一致性校验**需求，作为独立模块从主需求文档中拆分。

一致性校验的目标是验证源集群与目标集群之间的数据同步准确性，支持两种校验模式：

1. **精确校验模式（基于业务唯一 ID + 指纹快照）**：当消息携带业务唯一 ID（如 `keys`）时，通过时间窗口定义校验范围，分别从源和目标集群拉取同一时间段内的消息，基于 `{bizId: fingerprint}` 快照进行集合比对，精准识别消息丢失、消息冗余和内容篡改三类不一致。
2. **降级校验模式（元数据级别校验）**：当消息未携带业务唯一 ID 时，仅校验 Topic、Cons费者组数量等元数据级别的一致性——需要同步的 Topic 和 ConsumerGroup 必须在目标集群存在（目标可以多，但不能少）。

**核心设计原则：**

- **基于时间窗口而非 offset**：源集群与目标集群的 offset 不保证一致（重试、写入时机等原因），因此使用消息的存储时间戳定义校验范围
- **不一致容忍度可配置**：支持从 0%（零容忍）到 100%（完全容忍）的不一致比例配置，适应不同场景需求
- **无需全量拉取**：基于指纹聚合和集合比对，性能高效
- **可周期性运行**：支持定时自动执行
- **精准定位不一致**：输出消息级别的差异报告（丢失、冗余、篡改）

---

## 启动参数

以下启动参数专用于一致性校验功能，需在同步组件的启动命令中配置：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--consistencyCheckInterval <ms>` | 关闭 | 一致性校验定期执行间隔（毫秒），设置后开启周期性校验 |
| `--consistencyTimeWindowMinutes <n>` | 60 | 每次校验的时间窗口大小（分钟），即校验最近 N 分钟内的消息 |
| `--consistencyTolerancePercent <n>` | 0 | 不一致容忍度（0~100），0 表示零容忍，100 表示完全容忍；当不一致消息占比 ≤ 该值时视为通过 |
| `--consistencyBizIdField <field>` | keys | 业务唯一 ID 的取值字段，可选值为 `keys`、`uniqKey`（UNIQUE_KEY 属性）；若消息中该字段为空则自动降级为元数据校验模式 |
| `--consistencyParallelism <n>` | 4 | 校验并行线程数 |
| `--consistencyPageSize <n>` | 1000 | 每次从集群拉取消息的批次大小 |

---

## 需求

---

### 需求 1：一致性校验触发与入口

**用户故事：** 作为一名运维人员，我希望能通过 HTTP 接口手动触发或配置定时自动执行一致性校验，以便灵活地验证源集群与目标集群的数据同步准确性。

#### 验收标准

1. WHEN 组件运行时 THEN 系统 SHALL 提供 `GET /consistency` HTTP 接口，支持以下查询参数：
   - `topic`（可选）：指定校验的 Topic 名称，不指定则对所有同步中的 Topic 执行校验
   - `startTime`（可选）：校验时间窗口的起始时间（ISO-8601 格式），不指定则默认为 `当前时间 - consistencyTimeWindowMinutes`
   - `endTime`（可选）：校验时间窗口的结束时间（ISO-8601 格式），不指定则默认为当前时间
   - `tolerancePercent`（可选）：本次校验的不一致容忍度，覆盖启动参数的默认值
2. WHEN 配置了 `--consistencyCheckInterval <ms>` 时 THEN 系统 SHALL 启动后台定时任务，按指定间隔自动执行一致性校验（校验范围为最近 `--consistencyTimeWindowMinutes` 分钟内的消息）
3. WHEN 一致性校验正在执行时 THEN 系统 SHALL 拒绝新的校验请求（返回 HTTP 409），并在响应中提示"一致性校验正在执行中，请稍后重试"
4. WHEN 一致性校验开始时 THEN 系统 SHALL 打印 INFO 日志（"一致性校验开始：topics={topics}, timeWindow=[{startTime}, {endTime}], tolerancePercent={tolerancePercent}"）

---

### 需求 2：精确校验模式（基于业务唯一 ID + 指纹快照）

**用户故事：** 作为一名运维人员，我希望在消息携带业务唯一 ID 的场景下，通过时间窗口内的消息指纹快照精确比对，识别消息丢失、冗余和内容篡改，以便精准定位数据同步问题。

#### 验收标准

**Step 1：确定校验范围（基于时间窗口）**

1. WHEN 执行精确校验时 THEN 系统 SHALL 使用消息的 `storeTimestamp`（存储时间戳）定义校验范围，而非 offset；通过 RocketMQ 的按时间戳查询 API（`searchOffset`）分别从源集群和目标集群获取时间窗口内的消息
2. WHEN 确定校验范围时 THEN 系统 SHALL 对指定 Topic 的每个 Queue 分别查询 `[startTime, endTime]` 时间段对应的起止 offset，作为该 Queue 的拉取范围

**Step 2：拉取消息并构建指纹快照**

3. WHEN 对源集群某个 Topic 的某个 Queue 拉取消息时 THEN 系统 SHALL 使用 `DefaultLitePullConsumer` 按 offset 范围批量拉取消息（批次大小由 `--consistencyPageSize` 控制），逐条提取业务唯一 ID 和计算指纹
4. WHEN 提取业务唯一 ID 时 THEN 系统 SHALL 按 `--consistencyBizIdField` 配置的字段读取：
   - `keys`：读取 `MessageExt.getKeys()` 字段
   - `uniqKey`：读取消息 Properties 中的 `UNIQ_KEY` 属性
   - IF 该字段为空或 null THEN 标记该消息为"无业务 ID"，不纳入精确校验
5. WHEN 计算消息指纹时 THEN 系统 SHALL 生成确定性的 CRC32 指纹值，计算规则为：`fingerprint = CRC32(body + "#" + tags + "#" + keys)`，其中：
   - `body`：消息体的原始字节数组
   - `tags`：消息的 Tag 字段（为 null 时使用空字符串）
   - `keys`：消息的 Keys 字段（为 null 时使用空字符串）
   - **排除字段**：`bornHost`、`bornTimestamp`、`storeTimestamp`、`storeHost`、`brokerName` 等环境相关字段**不参与**指纹计算
6. WHEN 拉取完一个 Queue 的所有消息后 THEN 系统 SHALL 构建该 Queue 的指纹快照：`Map<String, Long> snapshot`，key 为业务唯一 ID（`bizId`），value 为消息指纹（`fingerprint`）；同一 `bizId` 出现多条消息时，保留最后一条的指纹
7. WHEN 对目标集群执行相同操作时 THEN 系统 SHALL 使用相同的时间窗口和拉取逻辑，构建目标集群的指纹快照

**Step 3：集合比对，识别三类不一致**

8. WHEN 源集群和目标集群的指纹快照均构建完成后 THEN 系统 SHALL 执行集合比对，识别以下三类不一致：
   - **消息丢失（MISSING）**：`bizId` 仅存在于源集群快照中（`sourceSnapshot.keySet() - targetSnapshot.keySet()`），表示消息未同步到目标集群
   - **消息冗余（REDUNDANT）**：`bizId` 仅存在于目标集群快照中（`targetSnapshot.keySet() - sourceSnapshot.keySet()`），表示目标集群存在源集群不存在的消息
   - **内容篡改（TAMPERED）**：`bizId` 同时存在于两个快照中，但 `fingerprint` 不同（`sourceSnapshot.get(bizId) != targetSnapshot.get(bizId)`），表示消息内容不一致
9. WHEN 计算不一致比例时 THEN 系统 SHALL 使用公式：`inconsistencyPercent = (missingCount + tamperedCount) / sourceSnapshotSize * 100`；冗余消息不计入不一致比例（目标可以多，但不能少）
10. WHEN `inconsistencyPercent <= consistencyTolerancePercent` 时 THEN 系统 SHALL 标记该 Topic 为 `CONSISTENT`（在容忍度范围内）
11. WHEN `inconsistencyPercent > consistencyTolerancePercent` 时 THEN 系统 SHALL 标记该 Topic 为 `INCONSISTENT`

**Step 4：处理无业务 ID 的消息**

12. WHEN 某个 Topic 在时间窗口内拉取的消息中，超过 50% 的消息缺少业务唯一 ID 时 THEN 系统 SHALL 自动将该 Topic 降级为**降级校验模式**（需求 3），并在报告中标记 `checkMode: "DEGRADED"`
13. WHEN 某个 Topic 在时间窗口内拉取的消息中，少于 50% 缺少业务唯一 ID 时 THEN 系统 SHALL 仅对有业务 ID 的消息执行精确校验，无业务 ID 的消息在报告中单独统计为 `noBizIdCount`

---

### 需求 3：降级校验模式（元数据级别校验）

**用户故事：** 作为一名运维人员，我希望在消息未携带业务唯一 ID 的场景下，仍能校验目标集群的元数据完整性——确保需要同步的 Topic 和 ConsumerGroup 在目标集群中都存在，以便在无法做消息级别对比的情况下，至少保证元数据层面的一致性。

#### 验收标准

1. WHEN 执行降级校验时 THEN 系统 SHALL 从源集群获取需要同步的 Topic 列表（根据 `--topics` 白名单配置，未配置则获取全部 Topic）
2. WHEN 获取到源集群 Topic 列表后 THEN 系统 SHALL 逐一检查这些 Topic 是否在目标集群存在（通过目标集群 NameServer 的 `GET_ROUTE_INFO_BY_TOPIC` 接口查询）
3. WHEN 源集群需要同步的 Topic 在目标集群**不存在**时 THEN 系统 SHALL 标记该 Topic 为 `MISSING_IN_TARGET`，打印 WARN 日志
4. WHEN 目标集群存在源集群未同步的额外 Topic 时 THEN 系统 SHALL **不视为错误**（目标可以多，但不能少），在报告中记录为 `extraTopicsInTarget`
5. WHEN 执行降级校验时 THEN 系统 SHALL 从源集群获取所有 ConsumerGroup 配置（通过 `GET_ALL_SUBSCRIPTIONGROUP_CONFIG` 接口），检查这些 ConsumerGroup 是否在目标集群存在
6. WHEN 源集群的 ConsumerGroup 在目标集群**不存在**时 THEN 系统 SHALL 标记该 ConsumerGroup 为 `MISSING_IN_TARGET`，打印 WARN 日志
7. WHEN 目标集群存在源集群没有的额外 ConsumerGroup 时 THEN 系统 SHALL **不视为错误**（目标可以多，但不能少）
8. WHEN 降级校验完成后 THEN 系统 SHALL 综合 Topic 和 ConsumerGroup 的缺失情况判定结果：
   - IF 所有需同步的 Topic 和 ConsumerGroup 在目标集群均存在 THEN 标记为 `CONSISTENT`
   - IF 存在任何缺失 THEN 标记为 `INCONSISTENT`
9. WHEN 降级校验模式下 THEN 系统 SHALL 同样支持不一致容忍度配置：`tolerancePercent` 基于缺失数量占需同步总数的比例计算

---

### 需求 4：不一致容忍度配置

**用户故事：** 作为一名运维人员，我希望能配置不一致的容忍度，从零容忍到完全容忍灵活调节，以便在不同业务场景下选择合适的校验严格度。

#### 验收标准

1. WHEN 启动参数中指定了 `--consistencyTolerancePercent <n>`（取值范围 0~100）时 THEN 系统 SHALL 将该值作为默认的不一致容忍度
2. WHEN `consistencyTolerancePercent` 设置为 **0** 时 THEN 系统 SHALL 执行**零容忍**校验——任何消息丢失或内容篡改都将标记为 `INCONSISTENT`
3. WHEN `consistencyTolerancePercent` 设置为 **100** 时 THEN 系统 SHALL 执行**完全容忍**校验——无论不一致比例多高，都标记为 `CONSISTENT`（仅记录日志，不告警）
4. WHEN 通过 `GET /consistency?tolerancePercent=X` 接口传入 `tolerancePercent` 时 THEN 系统 SHALL 使用接口参数值覆盖启动参数的默认值（仅对本次校验生效）
5. WHEN 不一致比例等于容忍度阈值时（`inconsistencyPercent == tolerancePercent`）THEN 系统 SHALL 视为 `CONSISTENT`（边界条件取等号）
6. WHEN 校验结果为 `CONSISTENT`（在容忍度范围内）但仍存在不一致消息时 THEN 系统 SHALL 打印 INFO 日志（"一致性校验通过（在容忍度范围内）：不一致比例 {inconsistencyPercent}%，容忍度 {tolerancePercent}%"），并在报告中如实输出不一致的详细信息

---

### 需求 5：一致性校验报告

**用户故事：** 作为一名运维人员，我希望一致性校验完成后能生成结构化的报告，包含不一致的详细信息，以便快速定位问题。

#### 验收标准

1. WHEN 一致性校验完成后 THEN 系统 SHALL 返回 JSON 格式的对比报告，包含以下字段：
   ```json
   {
     "checkTime": "ISO-8601 格式的校验时间",
     "duration": "本次校验耗时（毫秒）",
     "timeWindow": {
       "startTime": "校验时间窗口起始时间",
       "endTime": "校验时间窗口结束时间"
     },
     "tolerancePercent": "本次校验使用的容忍度",
     "overallStatus": "CONSISTENT / INCONSISTENT",
     "overallInconsistencyPercent": "整体不一致比例",
     "topicReports": [
       {
         "topic": "Topic 名称",
         "checkMode": "EXACT / DEGRADED",
         "status": "CONSISTENT / INCONSISTENT",
         "inconsistencyPercent": "该 Topic 的不一致比例",
         "sourceMessageCount": "源集群该时间窗口内的消息总数",
         "targetMessageCount": "目标集群该时间窗口内的消息总数",
         "noBizIdCount": "无业务 ID 的消息数量（仅精确模式）",
         "missingMessages": [
           {
             "bizId": "业务唯一 ID",
             "sourceOffset": "源集群中的 offset",
             "sourceQueueId": "源集群中的 Queue ID",
             "storeTimestamp": "存储时间戳"
           }
         ],
         "redundantMessages": [
           {
             "bizId": "业务唯一 ID",
             "targetOffset": "目标集群中的 offset",
             "targetQueueId": "目标集群中的 Queue ID"
           }
         ],
         "tamperedMessages": [
           {
             "bizId": "业务唯一 ID",
             "sourceFingerprint": "源集群指纹",
             "targetFingerprint": "目标集群指纹"
           }
         ]
       }
     ],
     "degradedReport": {
       "missingTopics": ["缺失的 Topic 列表"],
       "missingConsumerGroups": ["缺失的 ConsumerGroup 列表"],
       "extraTopicsInTarget": ["目标集群多出的 Topic 列表"],
       "extraConsumerGroupsInTarget": ["目标集群多出的 ConsumerGroup 列表"]
     }
   }
   ```
2. WHEN 报告中 `missingMessages` 或 `tamperedMessages` 列表过长（超过 1000 条）时 THEN 系统 SHALL 截断为前 1000 条，并在报告中标记 `truncated: true` 和 `totalCount` 总数
3. WHEN 一致性校验完成后 THEN 系统 SHALL 将报告同时写入日志（INFO 级别）和通过 HTTP 响应返回

---

### 需求 6：一致性校验监控指标

**用户故事：** 作为一名运维人员，我希望一致性校验的执行状态和结果能通过监控指标暴露，以便集成到现有的监控告警体系中。

#### 验收标准

1. WHEN 组件运行时 THEN 系统 SHALL 在 `/metrics` 接口中新增以下一致性校验指标：
   - **consistencyCheckCount**：累计执行一致性校验的次数
   - **consistencyCheckFailCount**：累计发现不一致（超过容忍度）的校验次数
   - **consistencyCheckPassCount**：累计通过（在容忍度范围内）的校验次数
   - **lastConsistencyCheckTime**：最近一次校验的时间戳（ISO-8601）
   - **lastConsistencyResult**：最近一次校验结果，枚举值 `CONSISTENT` / `INCONSISTENT` / `NOT_CHECKED`
   - **lastInconsistencyPercent**：最近一次校验的不一致比例
   - **consistencyCheckRunning**：当前是否正在执行校验（boolean）
   - **totalMissingMessageCount**：最近一次校验发现的消息丢失总数
   - **totalRedundantMessageCount**：最近一次校验发现的消息冗余总数
   - **totalTamperedMessageCount**：最近一次校验发现的内容篡改总数

2. WHEN 一致性校验发现不一致（超过容忍度）时 THEN 系统 SHALL 打印 WARN 日志，包含不一致的 Topic 列表、不一致比例、容忍度配置
3. WHEN 一致性校验连续 3 次发现不一致时 THEN 系统 SHALL 打印 ERROR 日志，提示数据同步可能存在持续性问题，建议人工介入排查
4. WHEN 一致性校验执行失败（如拉取消息超时、集群连接异常等）时 THEN 系统 SHALL 打印 ERROR 日志，将 `consistencyCheckErrorCount` 计数器加 1，不影响数据同步主流程
