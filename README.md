# IoT 设备接入网关（本地可运行）

一套**零外部中间件**的物联网设备接入网关：内置纯 JDK 实现的最小 MQTT 3.1.1 Broker，
同时提供 HTTP REST 接入；遥测、设备影子、命令下发在两种接入方式下归一化到同一套核心服务，
结论一致、行为可解释。

## 一条命令验证

```bash
mvn test
```

测试内嵌 MQTT Broker（随机端口）+ Spring Web（随机端口），不依赖外部 MQTT/数据库/真实设备。
日志中以 `[场景1]` ~ `[场景7]` 标明覆盖的业务场景及每个断言的结果。

本地手动演示（两个进程）：

```bash
mvn spring-boot:run                       # 终端1：启动网关（HTTP 8080 / MQTT 1883）
# 终端2：IDE 中运行 com.github.lechandonga.iotdevicetwingatewayjava.simulate.LocalSimulation
```

## 接入协议映射

| 业务动作 | MQTT（端口 1883） | HTTP（http://localhost:8080） |
|---|---|---|
| 设备鉴权 | CONNECT 报文 username=设备ID，password=密钥；遗嘱 topic=`devices/{id}/status` | 每个请求头 `X-Device-Id`、`X-Device-Secret` |
| 遥测上报 | PUBLISH QoS1 → `devices/{id}/telemetry` | POST `/api/v1/devices/{id}/telemetry` |
| 命令下发 | PUBLISH → `devices/{id}/cmd` | 设备 GET 长轮询 `/api/v1/devices/{id}/commands/pull?waitSeconds=n` |
| 命令确认 | PUBLISH → `devices/{id}/cmd/ack` | POST `/api/v1/devices/{id}/commands/{commandId}/ack` |
| 影子查询 | — | GET `/api/v1/devices/{id}/shadow` |
| 在线/离线 | CONNECT/DISCONNECT/TCP 断开（遗嘱） | POST `/connect`、`/disconnect` |

云端（业务侧）API：

- 注册设备：POST `/api/v1/cloud/devices`，体 `{"deviceId":"...","secret":"..."}`
- 设置期望：POST `/api/v1/cloud/devices/{id}/desired`，体为期望属性补丁
- 下发命令：POST `/api/v1/cloud/devices/{id}/commands?transport=MQTT|HTTP`
- 查询命令：GET `/api/v1/cloud/commands/{commandId}`
- 查询设备命令列表：GET `/api/v1/cloud/devices/{id}/commands`
- 命令生命周期事件流水：GET `/api/v1/cloud/commands/events`

### 拒绝原因码（两种接入一致，可区分）

| errorCode | 含义 | HTTP 状态码 | MQTT 表现 |
|---|---|---|---|
| `MISSING_CREDENTIALS` | 未提供凭据 | 401 | CONNACK 0x04 |
| `BAD_CREDENTIALS` | 密钥错误/身份不一致 | 401 | CONNACK 0x04 |
| `DEVICE_NOT_REGISTERED` | 设备未注册 | 403 | CONNACK 0x05 |
| `MALFORMED_PAYLOAD` | 报文非 JSON 对象、ts 非数字、properties 非对象 | 400 | Broker 丢弃并记录原因 |
| `GATEWAY_BUSY` | 超过全局并发处理能力 | 503 | 明确拒绝，不排队 |
| `COMMAND_QUEUE_FULL` | 单设备命令队列超限 | 503 | 明确拒绝 |
| `COMMAND_NOT_FOUND` | 命令不存在/不在等待确认 | 404 | — |

### 遥测缺省语义（稳定明确）

遥测报文归一化为 `{deviceId, ts, properties, transport}`：

- `ts` 缺失或 null：使用**网关接收时间**（毫秒）
- `properties` 缺失或 null：视为**空对象**
- 顶层必须是 JSON 对象；`ts` 必须是数字；`properties` 必须是对象，值必须为标量；否则 `MALFORMED_PAYLOAD`

## 影子同步规则

影子分 `reported`（设备实际值）与 `desired`（云端期望值），带单调递增 `version`：

- **只采纳更新鲜的上报**：仅当遥测 `ts` 严格大于影子已采纳时间戳时才合并进 `reported`；
  乱序/迟到遥测返回 `applied=false`（原因 `STALE_TELEMETRY`），影子不回退
- `desired` 为合并式补丁，由云端显式写入，不受遥测时序影响
- 同步状态：`desired` 中每个键都在 `reported` 中且值相等 → `IN_SYNC`；否则 `OUT_OF_SYNC`；
  无期望时默认 `IN_SYNC`
- 同一设备的所有影子读写由**按设备分片锁**串行化；不同设备互不阻塞

## 命令生命周期

```
QUEUED ──在线且可投递──▶ DELIVERED ──设备 ACK──▶ ACKED（含 result，终态，可查询）
   ▲                        │
   └──── ACK 超时回队首重试 ─┤（TIMED_OUT，attempts+1）
                            └── attempts ≥ gateway.command-max-attempts ──▶ EXPIRED（终态）
```

- 设备离线时下发的命令进入**有界 FIFO 队列**，绝不静默丢弃；队列满返回 `COMMAND_QUEUE_FULL`
- 重连（MQTT CONNECT 或 HTTP connect/长轮询到达）后**按原下发顺序**补齐
- 同一时刻每设备只允许一条命令处于等待 ACK（队首阻塞），从机制上保证顺序
- 超时未确认自动置于队首重试；超过 `gateway.command-max-attempts` 标记 `EXPIRED`
- 每次状态迁移都可经 `GET /api/v1/cloud/commands/{id}` 与 `/commands/events` 追溯

## 并发与过载保护

- **单设备分片锁**（`DeviceLockManager`）：并发上报/下发/ACK 串行化，杜绝丢更新与影子回退
- **全局容量闸门**（`gateway.capacity`，`IngestGate` 信号量）：超过处理能力立即返回
  `GATEWAY_BUSY`（HTTP 503），不做无界堆积
- **每设备命令队列上限**（`gateway.command-queue-capacity`）
- 故障隔离：单设备的畸形报文、ACK 异常只影响自身会话，其他设备接入不受影响

## 测试场景（`GatewayIntegrationTests`，共 7 个）

1. 鉴权拒绝：未注册/凭据错误/缺失凭据/畸形报文，原因码可区分（MQTT+HTTP）
2. 乱序/迟到遥测不覆盖新状态；缺省语义；MQTT 与 HTTP 结论一致
3. 离线命令排队，重连后按原顺序补齐并全部 ACK
4. 命令 ACK 超时、自动重试、超限 EXPIRED，状态全程可查询
5. 同一设备 12 线程 × 50 条并发上报与期望/实际交织写入：无丢更新、影子不回退
6. 过载明确返回 `GATEWAY_BUSY`；邻近设备不受影响；命令队列超限返回 `COMMAND_QUEUE_FULL`
7. 影子期望/实际分离与 IN_SYNC/OUT_OF_SYNC 转换；HTTP 长轮询命令链路与 MQTT 生命周期一致

## 关键配置（`application.properties`）

```properties
server.port=8080
gateway.mqtt.port=1883
gateway.capacity=64                  # 全局并发处理能力
gateway.command-queue-capacity=100   # 每设备命令队列上限
gateway.command-ack-timeout-ms=2000  # 命令 ACK 超时
gateway.command-max-attempts=3       # 最大投递次数
```

## 代码结构

- `model`：统一遥测/影子/命令模型与稳定错误码
- `registry`：设备注册凭据、在线连接状态
- `core`：遥测归一化、影子服务、命令生命周期、容量闸门、分片锁
- `mqtt`：内嵌 MQTT 3.1.1 Broker 与轻量客户端（CONNECT/QoS1/遗嘱/通配符订阅）
- `http`：设备侧与云端 REST API
- `simulate`：本地设备模拟器（HTTP + MQTT）与端到端演示 `LocalSimulation`
