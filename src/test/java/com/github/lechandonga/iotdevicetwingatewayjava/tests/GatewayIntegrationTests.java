package com.github.lechandonga.iotdevicetwingatewayjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.iotdevicetwingatewayjava.core.CommandService;
import com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayProperties;
import com.github.lechandonga.iotdevicetwingatewayjava.core.ShadowService;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Telemetry;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.MqttBroker;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.MqttClient;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.ConnectionRegistry;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.DeviceRegistry;
import com.github.lechandonga.iotdevicetwingatewayjava.simulate.HttpDeviceSimulator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 网关全链路集成测试：MQTT 与 HTTP 两种接入、鉴权、乱序、离线补齐、并发与过载。
 * 运行：mvn test （无需外部中间件）
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.MethodName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GatewayIntegrationTests {

    private static final Logger log = LoggerFactory.getLogger(GatewayIntegrationTests.class);

    @LocalServerPort
    private int httpPort;

    @Autowired
    private MqttBroker mqttBroker;
    @Autowired
    private DeviceRegistry deviceRegistry;
    @Autowired
    private ConnectionRegistry connectionRegistry;
    @Autowired
    private ShadowService shadowService;
    @Autowired
    private CommandService commandService;
    @Autowired
    private GatewayProperties gatewayProperties;

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();

    private final List<MqttClient> mqttClients = new ArrayList<>();
    private final List<HttpDeviceSimulator> httpSimulators = new ArrayList<>();

    private String baseUrl;

    @BeforeEach
    void setUp() {
        mqttClients.forEach(MqttClient::close);
        mqttClients.clear();
        httpSimulators.forEach(HttpDeviceSimulator::close);
        httpSimulators.clear();
        commandService.clear();
        shadowService.clear();
        connectionRegistry.clear();
        deviceRegistry.clear();
        baseUrl = "http://localhost:" + httpPort;
        log.info("---------- 测试环境：HTTP={} MQTT={} 容量闸门={} 命令队列上限={} ACK超时={}ms ----------",
                httpPort, mqttBroker.getBoundPort(), gatewayProperties.getCapacity(),
                gatewayProperties.getCommandQueueCapacity(), gatewayProperties.getCommandAckTimeoutMs());
    }

    @Test
    void scenario01_authenticationRejectionsAreDistinguishable() throws Exception {
        log.info("[场景1] 鉴权拒绝：未注册 / 凭据错误 / 缺失凭据 / 报文畸形");
        registerDevice("dev-ok", "good-secret");

        MqttClient unregistered = mqttClient("dev-ghost", "whatever");
        String error = unregistered.connect();
        assertTrue(error != null && error.contains("0x5"), "未注册设备应被拒绝（0x05），实际：" + error);
        log.info("[场景1] MQTT 未注册设备被拒绝：{}", error);

        MqttClient badSecret = mqttClient("dev-ok", "wrong-secret");
        error = badSecret.connect();
        assertTrue(error != null && error.contains("0x4"), "错误凭据应被拒绝（0x04），实际：" + error);
        log.info("[场景1] MQTT 凭据错误被拒绝：{}", error);

        HttpResponse<String> noCreds = rawHttp("POST", "/api/v1/devices/dev-ok/telemetry",
                Map.of(), "{\"ts\":1,\"properties\":{}}");
        assertEquals(401, noCreds.statusCode());
        assertTrue(noCreds.body().contains("MISSING_CREDENTIALS"));
        log.info("[场景1] HTTP 缺失凭据 401：{}", oneLine(noCreds.body()));

        HttpResponse<String> badCreds = rawHttp("POST", "/api/v1/devices/dev-ok/telemetry",
                Map.of("X-Device-Secret", "wrong"), "{\"ts\":1,\"properties\":{}}");
        assertEquals(401, badCreds.statusCode());
        assertTrue(badCreds.body().contains("BAD_CREDENTIALS"));
        log.info("[场景1] HTTP 凭据错误 401：{}", oneLine(badCreds.body()));

        HttpResponse<String> unknown = rawHttp("POST", "/api/v1/devices/dev-ghost/telemetry",
                Map.of("X-Device-Secret", "x"), "{}");
        assertEquals(403, unknown.statusCode());
        assertTrue(unknown.body().contains("DEVICE_NOT_REGISTERED"));
        log.info("[场景1] HTTP 未注册设备 403：{}", oneLine(unknown.body()));

        HttpResponse<String> malformed = rawHttp("POST", "/api/v1/devices/dev-ok/telemetry",
                Map.of("X-Device-Secret", "good-secret"), "this-is-not-json");
        assertEquals(400, malformed.statusCode());
        assertTrue(malformed.body().contains("MALFORMED_PAYLOAD"));
        log.info("[场景1] HTTP 畸形报文 400：{}", oneLine(malformed.body()));
    }

    @Test
    void scenario02_outOfOrderTelemetryDoesNotRollBack() throws Exception {
        log.info("[场景2] 乱序/迟到遥测不回退；缺省语义稳定；MQTT/HTTP 结论一致");
        registerDevice("dev-mqtt", "s");
        registerDevice("dev-http", "s");

        MqttClient mqtt = connectMqtt("dev-mqtt", "s");
        long now = System.currentTimeMillis();
        publishMqtt(mqtt, "dev-mqtt", now + 1000, Map.of("temp", 30));
        publishMqtt(mqtt, "dev-mqtt", now + 500, Map.of("temp", 20));

        long mqttLastTs = now + 1000;
        assertTrue(waitUntil(3000, () ->
                getShadowQuiet("dev-mqtt").path("lastUpdatedTs").asLong() == mqttLastTs),
                "MQTT 遥测处理应在限定时间内完成");
        JsonNode mqttShadow = getShadow("dev-mqtt");
        assertEquals(30, mqttShadow.path("reported").path("temp").asInt(), "旧遥测不得覆盖新状态");
        log.info("[场景2] MQTT reported={}（30 保留，迟到的 20 被忽略）", mqttShadow.path("reported"));

        HttpDeviceSimulator simulator = httpSimulator("dev-http", "s");
        simulator.connect();
        HttpResponse<String> first = simulator.rawTelemetry(
                "{\"ts\":" + (now + 2000) + ",\"properties\":{\"humidity\":80}}");
        HttpResponse<String> late = simulator.rawTelemetry(
                "{\"ts\":" + (now + 1000) + ",\"properties\":{\"humidity\":10}}");
        assertEquals(200, first.statusCode());
        assertEquals(200, late.statusCode());
        assertFalse(mapper.readTree(late.body()).path("applied").asBoolean(),
                "迟到遥测响应应标记 applied=false");
        JsonNode httpShadow = getShadow("dev-http");
        assertEquals(80, httpShadow.path("reported").path("humidity").asInt());
        log.info("[场景2] HTTP reported={} applied=false 拒绝回退", httpShadow.path("reported"));

        assertEquals(200, simulator.rawTelemetry("{\"properties\":{}}").statusCode());
        assertEquals(200, simulator.rawTelemetry("{}").statusCode());
        log.info("[场景2] ts/properties 缺失按明确缺省语义接受（ts=网关时间，properties=空）");
    }

    @Test
    void scenario03_offlineCommandsReplayInOrderAfterReconnect() throws Exception {
        log.info("[场景3] 离线命令不丢弃，MQTT 重连后按原顺序补齐并确认");
        registerDevice("dev-reconnect", "s");

        MqttClient device = connectMqtt("dev-reconnect", "s");
        CountDownLatch firstAck = new CountDownLatch(1);
        device.subscribe("devices/dev-reconnect/cmd", (topic, payload) -> {
            try {
                JsonNode command = mapper.readTree(payload);
                device.publishQos1("devices/dev-reconnect/cmd/ack",
                        ackJson(command.path("commandId").asText()));
                firstAck.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(200);

        JsonNode cmd1 = issueCommand("dev-reconnect", "MQTT", "cmd-first", Map.of());
        assertTrue(firstAck.await(2, TimeUnit.SECONDS), "在线命令应及时确认");

        mqttClients.remove(device);
        device.close();
        Thread.sleep(300);
        assertFalse(connectionRegistry.isOnline("dev-reconnect"));

        JsonNode cmd2 = issueCommand("dev-reconnect", "MQTT", "cmd-offline-1", Map.of());
        JsonNode cmd3 = issueCommand("dev-reconnect", "MQTT", "cmd-offline-2", Map.of());
        assertEquals("QUEUED", cmd2.path("status").asText());
        assertEquals("QUEUED", cmd3.path("status").asText());
        log.info("[场景3] 离线期间两条命令保持 QUEUED");

        MqttClient reconnected = connectMqtt("dev-reconnect", "s");
        List<String> replayOrder = new ArrayList<>();
        CountDownLatch replayLatch = new CountDownLatch(2);
        reconnected.subscribe("devices/dev-reconnect/cmd", (topic, payload) -> {
            try {
                JsonNode command = mapper.readTree(payload);
                replayOrder.add(command.path("name").asText());
                reconnected.publishQos1("devices/dev-reconnect/cmd/ack",
                        ackJson(command.path("commandId").asText()));
                replayLatch.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(replayLatch.await(5, TimeUnit.SECONDS),
                "重连后应补齐 2 条命令，实际：" + replayOrder);
        assertEquals(List.of("cmd-offline-1", "cmd-offline-2"), replayOrder,
                "补齐顺序必须与下发顺序一致");

        for (JsonNode command : List.of(cmd1, cmd2, cmd3)) {
            JsonNode queried = queryCommand(command.path("commandId").asText());
            assertEquals("ACKED", queried.path("status").asText());
            assertEquals("DONE", queried.path("result").asText());
        }
        log.info("[场景3] 补齐顺序={}，全部 ACKED 且结果可查询", replayOrder);
    }

    @Test
    void scenario04_commandTimeoutRetryAndExpiryAreQueryable() throws Exception {
        log.info("[场景4] ACK 超时 -> 回到队首重试 -> 超限 EXPIRED");
        registerDevice("dev-noack", "s");
        MqttClient device = connectMqtt("dev-noack", "s");
        device.subscribe("devices/dev-noack/cmd", (topic, payload) -> {
        });
        Thread.sleep(200);

        JsonNode command = issueCommand("dev-noack", "MQTT", "lost-ack", Map.of());
        String commandId = command.path("commandId").asText();

        long timeoutMs = gatewayProperties.getCommandAckTimeoutMs();
        int maxAttempts = gatewayProperties.getCommandMaxAttempts();
        Thread.sleep(timeoutMs * maxAttempts + 1200);

        JsonNode finalState = queryCommand(commandId);
        assertEquals("EXPIRED", finalState.path("status").asText(),
                "多次超时后应为 EXPIRED，实际：" + finalState.path("status").asText());
        assertEquals(maxAttempts, finalState.path("attempts").asInt());
        assertTrue(commandService.eventLog().stream().anyMatch(line -> line.contains("TIMED_OUT")),
                "事件流水应记录 TIMED_OUT");
        log.info("[场景4] 命令 {} 投递 {} 次后 EXPIRED，TIMED_OUT/重试过程可查询", commandId, maxAttempts);
    }

    @Test
    void scenario05_concurrentWritesDoNotLoseUpdates() throws Exception {
        log.info("[场景5] 同一设备并发上报+期望写入：无丢更新、影子不回退");
        registerDevice("dev-concurrent", "s");
        MqttClient device = connectMqtt("dev-concurrent", "s");

        int writers = 12;
        int perWriter = 50;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        ExecutorService mqttWriter = Executors.newSingleThreadExecutor();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers);

        for (int writer = 0; writer < writers; writer++) {
            int writerIndex = writer;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        final int sequence = writerIndex * perWriter + i + 1;
                        mqttWriter.submit(() -> {
                            try {
                                publishMqtt(device, "dev-concurrent", 1_000_000L + sequence,
                                        Map.of("counter", sequence));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }).get(20, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();
        mqttWriter.shutdown();
        assertTrue(mqttWriter.awaitTermination(10, TimeUnit.SECONDS));

        assertTrue(waitUntil(5000, () -> getShadowQuiet("dev-concurrent")
                .path("reported").path("counter").asInt() == writers * perWriter),
                "600 条并发上报后 counter 必须等于最大序号，证明无丢更新且未回退");
        JsonNode counterShadow = getShadow("dev-concurrent");
        assertEquals(writers * perWriter, counterShadow.path("reported").path("counter").asInt());

        CountDownLatch mixed = new CountDownLatch(2);
        Thread desiredWriter = new Thread(() -> {
            try {
                for (int i = 0; i < 30; i++) {
                    shadowService.setDesired("dev-concurrent", Map.of("target" + (i % 5), i));
                }
            } finally {
                mixed.countDown();
            }
        });
        Thread reporter = new Thread(() -> {
            try {
                for (int i = 0; i < 30; i++) {
                    shadowService.applyTelemetry(new Telemetry("dev-concurrent",
                            2_000_000L + i, Map.of("live" + (i % 5), i), "DIRECT"));
                }
            } finally {
                mixed.countDown();
            }
        });
        desiredWriter.start();
        reporter.start();
        assertTrue(mixed.await(10, TimeUnit.SECONDS));

        JsonNode shadow = getShadow("dev-concurrent");
        assertEquals(writers * perWriter, shadow.path("reported").path("counter").asInt(),
                "期望写入与额外上报交织时，counter 仍不得回退");
        for (int i = 0; i < 5; i++) {
            assertTrue(shadow.path("desired").has("target" + i), "desired 键 target" + i + " 不应丢失");
            assertTrue(shadow.path("reported").has("live" + i), "reported 键 live" + i + " 不应丢失");
        }
        log.info("[场景5] 并发结束 counter={} reported键数={} desired键数={} 版本={}，无丢更新/回退",
                shadow.path("reported").path("counter").asInt(),
                shadow.path("reported").size(), shadow.path("desired").size(),
                shadow.path("version").asLong());
    }

    @Test
    void scenario06_overloadRejectedExplicitlyAndDevicesIsolated() throws Exception {
        log.info("[场景6] 过载明确拒绝（GATEWAY_BUSY）；单设备异常/过载不影响其他设备");
        registerDevice("dev-busy", "s");
        registerDevice("dev-neighbor", "s");

        long savedDelay = gatewayProperties.getProcessingDelayMs();
        gatewayProperties.setProcessingDelayMs(40);
        int capacity = gatewayProperties.getCapacity();
        int attempts = capacity * 8;
        AtomicInteger busyCount = new AtomicInteger();
        AtomicInteger acceptedCount = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(attempts);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(attempts);
        for (int i = 0; i < attempts; i++) {
            int seq = i;
            pool.submit(() -> {
                try {
                    start.await();
                    HttpResponse<String> response = rawHttp("POST",
                            "/api/v1/devices/dev-busy/telemetry",
                            Map.of("X-Device-Secret", "s"),
                            "{\"ts\":" + (3_000_000L + seq) + ",\"properties\":{\"k\":" + seq + "}}");
                    if (response.statusCode() == 503 && response.body().contains("GATEWAY_BUSY")) {
                        busyCount.incrementAndGet();
                    } else if (response.statusCode() == 200) {
                        acceptedCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    finished.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(finished.await(30, TimeUnit.SECONDS));
        pool.shutdown();
        gatewayProperties.setProcessingDelayMs(savedDelay);
        assertTrue(busyCount.get() > 0, "容量被打满时必须出现 GATEWAY_BUSY 拒绝，而不是堆积");
        log.info("[场景6] 高并发 {} 个请求：接受 {}，明确拒绝 GATEWAY_BUSY {}",
                attempts, acceptedCount.get(), busyCount.get());

        HttpDeviceSimulator neighbor = httpSimulator("dev-neighbor", "s");
        neighbor.connect();
        HttpResponse<String> neighborResponse = neighbor.post("/connect", null);
        assertEquals(200, neighborResponse.statusCode(), "邻近设备接入不得受过载设备影响");
        assertEquals(200, neighbor.rawTelemetry(
                "{\"ts\":1,\"properties\":{\"ok\":true}}").statusCode());
        JsonNode neighborShadow = getShadow("dev-neighbor");
        assertTrue(neighborShadow.path("reported").path("ok").asBoolean());
        log.info("[场景6] 邻近设备 dev-neighbor 正常上报，影子={}，设备间隔离有效",
                neighborShadow.path("reported"));

        // 命令队列有界：超出上限明确 COMMAND_QUEUE_FULL
        registerDevice("dev-queue", "s");
        int queueCapacity = gatewayProperties.getCommandQueueCapacity();
        boolean sawQueueFull = false;
        for (int i = 0; i < queueCapacity + 3; i++) {
            HttpResponse<String> response = rawHttp("POST",
                    "/api/v1/cloud/devices/dev-queue/commands?transport=MQTT",
                    Map.of(), "{\"name\":\"c" + i + "\",\"payload\":{}}");
            if (response.statusCode() == 503 && response.body().contains("COMMAND_QUEUE_FULL")) {
                sawQueueFull = true;
            }
        }
        assertTrue(sawQueueFull, "命令队列超限时必须返回 COMMAND_QUEUE_FULL");
        log.info("[场景6] 离线设备命令队列达到上限 {} 后，超额命令被明确拒绝", queueCapacity);
    }

    @Test
    void scenario07_shadowDesiredReportedSyncAcrossTransports() throws Exception {
        log.info("[场景7] 影子期望/实际分离与同步状态，HTTP 命令长轮询链路与 MQTT 结论一致");
        registerDevice("dev-shadow", "s");

        setDesired("dev-shadow", Map.of("mode", "eco", "led", "on"));
        JsonNode shadow = getShadow("dev-shadow");
        assertEquals("OUT_OF_SYNC", shadow.path("syncState").asText());
        log.info("[场景7] 设置 desired 后未上报 -> OUT_OF_SYNC");

        MqttClient device = connectMqtt("dev-shadow", "s");
        publishMqtt(device, "dev-shadow", System.currentTimeMillis(), Map.of("mode", "eco"));
        shadow = getShadow("dev-shadow");
        assertEquals("OUT_OF_SYNC", shadow.path("syncState").asText(), "仅部分 desired 被满足");

        publishMqtt(device, "dev-shadow", System.currentTimeMillis() + 1, Map.of("led", "on"));
        shadow = getShadow("dev-shadow");
        assertEquals("IN_SYNC", shadow.path("syncState").asText(), "desired 全部被 reported 满足");
        log.info("[场景7] reported 完整满足 desired -> IN_SYNC");

        // HTTP 链路命令下发/长轮询/ACK 走通
        registerDevice("dev-http-cmd", "s");
        HttpDeviceSimulator httpDevice = httpSimulator("dev-http-cmd", "s");
        httpDevice.connect();
        httpDevice.startPolling();
        JsonNode command = issueCommand("dev-http-cmd", "HTTP", "set-led", Map.of("on", true));
        String commandId = command.path("commandId").asText();

        boolean acked = waitUntil(3000, () ->
                "ACKED".equals(queryCommandQuiet(commandId).path("status").asText()));
        assertTrue(acked, "HTTP 长轮询命令应在设备 ACK 后变为 ACKED");
        JsonNode finished = queryCommand(commandId);
        assertEquals("EXECUTED", finished.path("result").asText());
        log.info("[场景7] HTTP 命令 {} 经长轮询投递并 ACKED（result=EXECUTED），与 MQTT 生命周期一致", commandId);
    }

    // ---------------- 辅助方法 ----------------

    private void registerDevice(String deviceId, String secret) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/cloud/devices"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        mapper.writeValueAsString(Map.of("deviceId", deviceId, "secret", secret))))
                .build();
        http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private MqttClient mqttClient(String deviceId, String secret) {
        MqttClient client = new MqttClient("127.0.0.1", mqttBroker.getBoundPort(),
                deviceId + "-" + System.nanoTime(), deviceId, secret,
                "devices/" + deviceId + "/status", "OFFLINE");
        mqttClients.add(client);
        return client;
    }

    private MqttClient connectMqtt(String deviceId, String secret) throws Exception {
        MqttClient client = mqttClient(deviceId, secret);
        String error = client.connect();
        if (error != null) {
            throw new IllegalStateException("MQTT 连接失败: " + error);
        }
        Thread.sleep(100);
        return client;
    }

    private void publishMqtt(MqttClient client, String deviceId, long ts, Map<String, Object> props)
            throws Exception {
        client.publishQos1("devices/" + deviceId + "/telemetry",
                mapper.writeValueAsBytes(Map.of("ts", ts, "properties", props)));
    }

    private byte[] ackJson(String commandId) throws Exception {
        return mapper.writeValueAsBytes(Map.of("commandId", commandId, "result", "DONE"));
    }

    private HttpDeviceSimulator httpSimulator(String deviceId, String secret) {
        HttpDeviceSimulator simulator = new HttpDeviceSimulator(baseUrl, deviceId, secret);
        httpSimulators.add(simulator);
        return simulator;
    }

    private HttpResponse<String> rawHttp(String method, String path,
                                         Map<String, String> headers, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10));
        headers.forEach(builder::header);
        if ("POST".equals(method)) {
            builder.header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body));
        } else {
            builder.GET();
        }
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode getShadow(String deviceId) throws Exception {
        HttpResponse<String> response = rawHttp("GET",
                "/api/v1/cloud/devices/" + deviceId + "/shadow", Map.of(), null);
        return mapper.readTree(response.body());
    }

    private JsonNode setDesired(String deviceId, Map<String, Object> desired) throws Exception {
        HttpResponse<String> response = rawHttp("POST",
                "/api/v1/cloud/devices/" + deviceId + "/desired", Map.of(),
                mapper.writeValueAsString(desired));
        return mapper.readTree(response.body());
    }

    private JsonNode issueCommand(String deviceId, String transport, String name,
                                  Map<String, Object> payload) throws Exception {
        HttpResponse<String> response = rawHttp("POST",
                "/api/v1/cloud/devices/" + deviceId + "/commands?transport=" + transport,
                Map.of(), mapper.writeValueAsString(Map.of("name", name, "payload", payload)));
        assertEquals(200, response.statusCode(), "命令下发应成功：" + response.body());
        return mapper.readTree(response.body());
    }

    private JsonNode queryCommand(String commandId) throws Exception {
        HttpResponse<String> response = rawHttp("GET",
                "/api/v1/cloud/commands/" + commandId, Map.of(), null);
        assertEquals(200, response.statusCode());
        return mapper.readTree(response.body());
    }

    private JsonNode queryCommandQuiet(String commandId) {
        try {
            return queryCommand(commandId);
        } catch (Exception e) {
            return mapper.createObjectNode();
        }
    }

    private JsonNode getShadowQuiet(String deviceId) {
        try {
            return getShadow(deviceId);
        } catch (Exception e) {
            return mapper.createObjectNode();
        }
    }

    private boolean waitUntil(long timeoutMs, CheckedCondition condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.matches()) {
                    return true;
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(50);
        }
        try {
            return condition.matches();
        } catch (Exception e) {
            return false;
        }
    }

    private String oneLine(String json) {
        return json.replaceAll("\\s+", " ").trim();
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean matches() throws Exception;
    }
}
