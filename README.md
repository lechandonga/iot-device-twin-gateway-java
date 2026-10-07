# IoT 设备接入网关（Java，本地可运行）

一套**不依赖任何外部中间件或真实设备**即可在本地跑通的物联网设备接入网关：
用进程内组件模拟 MQTT Broker、HTTP 长轮询会话与设备进程，完整实现
遥测归一化、设备影子、命令下发与确认、超时重试、离线补齐、并发安全与过载拒绝。

- 接入方式：MQTT（进程内嵌 Broker）+ HTTP（REST + 命令长轮询）
- 两种接入方式映射到同一个核心（actor 模型），业务结论一致、拒绝原因一致
- 构建：Spring Boot 3 / Java 21；测试：JUnit 5 + Spring Boot Test

## 一条命令验证

```bash
mvn verify
```

该命令会顺序执行 15 个测试（见下文“测试覆盖”）。日志中会逐条打印

- `==== 场景：... ====`：当前覆盖的业务场景
- `[通过] ...` / `[失败] ...`：每条断言的结果

运行本地端到端演示（启动网关 + 内置设备进程，自动演示在线/离线/重连/补齐）：

```bash
mvn -DskipTests package
java -jar target/iot-device-twin-gateway-java-1.0-SNAPSHOT.jar --spring.profiles.active=demo
# 或 mvn spring-boot:run -Dspring-boot.run.arguments=--spring.profiles.active=demo
```

## 接入协议映射

MQTT 与 HTTP 在适配层完成解析后，统一转换为 `TelemetryIngest` / `Command` 模型进入核心，
因此鉴权、缺省、旧数据保护、影子与命令状态机的规则完全相同。

| 业务动作 | MQTT（内嵌 Broker） | HTTP |
|---|---|---|
| 设备身份 | CONNECT：username=`deviceId`，password=`secret`；支持遗嘱 | 请求头 `X-Device-Secret` |
| 上线 | CONNECT + 订阅 `devices/{id}/commands/#` | 开始对 `/api/device/{id}/commands` 长轮询 |
| 遥测上报 | PUBLISH `devices/{id}/telemetry` | POST `/api/device/{id}/telemetry` |
| 命令下发 | PUBLISH `devices/{id}/commands/{commandId}` | 长轮询 GET 返回 commands 数组 |
| 命令确认 | PUBLISH `devices/{id}/cmd/ack` | POST `/api/device/{id}/commands/{commandId}/ack` |
| 离线 | 遗嘱消息（retained `offline`）/ 主动断连 | DELETE `/api/device/{id}/commands` 或停止轮询 |
| 北向管理 | —（HTTP 统一承载） | `/api/admin/**`，请求头 `X-Admin-Token` |

北向接口：

| 接口 | 说明 |
|---|---|
| POST `/api/admin/devices/{id}/register` | 注册设备（body `{"secret":"..."}`） |
| POST `/api/admin/devices/{id}/commands` | 下发命令（body `{"name","params","desired"}`） |
| GET `/api/admin/devices/{id}/commands[/{cid}]` | 命令生命周期查询 |
| GET `/api/admin/devices/{id}/shadow` | 查询影子 |
| POST `/api/admin/devices/{id}/desired` | 直接写期望值 |

### 拒绝原因（两种协议同一套稳定取值）

| 原因码 | 含义 | HTTP 状态码 |
|---|---|---|
| `DEVICE_NOT_REGISTERED` | 设备未注册 | 404 |
| `BAD_CREDENTIALS` | 设备凭据错误 | 401 |
| `MALFORMED_PAYLOAD` | 报文畸形 / 必填缺失 / deviceId 与路径不一致 | 400 |
| `DEVICE_OVERLOADED` | 单设备串行队列已满，明确拒绝 | 503 |
| `GATEWAY_OVERLOADED` | 全局在飞容量已满 | 503 |
| `UNAUTHORIZED` | 北向管理凭据缺失或错误 | 401 |
| `UNKNOWN_COMMAND` | 命令不存在 | 404 |
| `CONFLICT` | 重复注册 | 409 |

## 遥测归一化与缺省语义

归一化在**核心层**完成（`DeviceActor.ingest`），MQTT/HTTP 行为一致：

- `deviceId` 必填，且必须与主题/路径一致；报文不可解析 → `MALFORMED_PAYLOAD`
- `properties` 缺失或为 null → 空对象 `{}`
- `ts` 缺失 → 网关接收时刻
- `seq` 缺失 → 该设备已见最大序号 +1（无历史时为 0）

**旧数据保护（乱序/迟到）**：满足任一条件的遥测不会覆盖影子，返回
`STALE_TELEMETRY_IGNORED`（HTTP 200 且 `accepted=false`；MQTT 记录日志）：

1. `seq < 影子当前 reportedSeq`；
2. `seq == reportedSeq` 且 `ts < reportedTs`。

## 设备影子同步规则

影子由 `desired`（期望，北向写）与 `reported`（实际，设备遥测/ack 写）组成，
并维护版本号、最新序号/时间戳与差异 `delta`：

- `OUT_OF_SYNC`：存在期望值与实际值不一致的键（`delta` 非空）
- `SYNCING`：差异已清空，但仍有 QUEUED / IN_FLIGHT / TIMEOUT 的命令
- `IN_SYNC`：无差异且无在途命令

## 命令生命周期

```
QUEUED ──(上线/重连后按 createdAt 顺序投递)──> IN_FLIGHT ──ack──> ACKED（成功终态）
  │                                              │
  └──────────────< 超时未 ack：TIMEOUT <──────────┘
                    │ 未达最大次数：重投（优先队头，其余顺序不变）
                    └ 达到最大次数：FAILED（失败终态，原因 ack timeout, retries exhausted）
```

- 设备**离线**时命令进入 `QUEUED` 持久保存在设备 actor 中，绝不静默丢弃；
- 重连后严格按入队顺序补齐（`pendingQueue` 先进先出）；
- 连接在 ack 前丢失：`IN_FLIGHT → QUEUED`，重连后重新投递；
- 超时由网关扫描线程判定（默认 1500ms，最多 3 次尝试，均可配置）；
- ack 可携带 `reported` 补丁，用于驱动影子向 IN_SYNC 收敛；重复 ack 幂等。

命令查询返回：`status / attempts / maxAttempts / createdAt / lastDispatchedAt /
ackedAt / ackResult / lastError`，确认、超时、重试结果全程可查。

## 并发安全与故障隔离

- 每个设备一个**单线程 actor**：遥测、期望、命令、ack、连接事件全部串行处理，
  同一设备并发上报/下发不会丢更新、影子不会回退（有专门并发测试）。
- 每设备**有界队列**（默认 64）+ Abort 拒绝策略：超出能力立即返回
  `DEVICE_OVERLOADED`，不悄悄堆积。
- 全局在飞许可（默认 1024）：超界返回 `GATEWAY_OVERLOADED`。
- 设备之间线程独立，一个设备阻塞/过载不影响其他设备接入（有专门隔离测试）。

## 测试覆盖

`src/test/java/.../tests/` 下：

- `MqttScenarioTests`：MQTT 鉴权拒绝（未注册/错凭据）、畸形报文、乱序遥测、
  缺省语义、离线命令按序补齐、超时重试可查、遗嘱离线、同设备并发写入
- `HttpScenarioTests`：HTTP 鉴权/畸形、归一化与旧数据保护、长轮询离线补齐与 ack、
  影子同步状态转换、并发上报一致
- `ProtocolParityTests`：同一遥测序列经 MQTT / HTTP 归一化后影子完全一致
- `OverloadIsolationTests`：设备级 `DEVICE_OVERLOADED`、网关级
  `GATEWAY_OVERLOADED`、故障设备不影响其他设备、缺省语义稳定性

关键配置（`application.properties`，测试可覆盖）：

```properties
gateway.per-device-queue-capacity=64
gateway.global-inflight-limit=1024
gateway.command-ack-timeout-ms=1500
gateway.command-max-attempts=3
gateway.timeout-sweep-interval-ms=100
gateway.admin-token=admin-token
```

## 主要代码结构

- `model/`：统一领域模型与稳定拒绝原因
- `core/`：`GatewayCore`（门面）、`DeviceActor`（每设备串行状态机）、容量配置
- `mqtt/`：进程内 MQTT Broker（含遗嘱/通配符/retained）与 MQTT 适配层
- `http/`：设备 REST、长轮询会话、北向管理接口、统一错误处理
- `simulator/`：MQTT/HTTP 设备进程模拟器（在线、离线、重连、收命令、ack）
- `demo/LocalDemoRunner`：`demo` profile 下的端到端场景演示
