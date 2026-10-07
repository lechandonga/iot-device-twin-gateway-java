package com.github.lechandonga.iotdevicetwingatewayjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.lechandonga.iotdevicetwingatewayjava.http.JsonSupport;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.CommandStatus;
import com.github.lechandonga.iotdevicetwingatewayjava.simulator.HttpDeviceSimulator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
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
import java.util.Map;

import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.assertEquals;
import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.assertTrue;
import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.waitFor;

/**
 * HTTP 接入场景测试（真实启动本地 Web 端口，无外部中间件）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HttpScenarioTests {

    private static final Logger log = LoggerFactory.getLogger("HTTP场景");

    @LocalServerPort
    private int port;

    @Autowired
    private com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayCore core;

    private final HttpClient http = HttpClient.newHttpClient();
    private String base;

    @BeforeAll
    void init() throws Exception {
        base = "http://localhost:" + port;
        register("http-dev", "secret-h");
        register("http-cmd", "secret-cmd");
    }

    private void register(String id, String secret) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/admin/devices/" + id + "/register"))
                        .header("X-Admin-Token", "admin-token")
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"secret\":\"" + secret + "\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode(), "注册设备 " + id);
    }

    private HttpResponse<String> telemetry(String id, String secret, String rawJson) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base + "/api/device/" + id + "/telemetry"))
                        .header("X-Device-Secret", secret)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(rawJson)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @Order(1)
    void authAndMalformedRejections() throws Exception {
        log.info("==== 场景：HTTP 鉴权拒绝与畸形报文（原因码与 MQTT 完全一致） ====");
        HttpResponse<String> unknown = telemetry("nobody", "x",
                "{\"properties\":{\"temp\":1}}");
        assertEquals(404, unknown.statusCode(), "未注册 -> 404");
        assertTrue(unknown.body().contains("DEVICE_NOT_REGISTERED"), "原因码 DEVICE_NOT_REGISTERED", unknown.body());

        HttpResponse<String> bad = telemetry("http-dev", "wrong",
                "{\"properties\":{\"temp\":1}}");
        assertEquals(401, bad.statusCode(), "凭据错误 -> 401");
        assertTrue(bad.body().contains("BAD_CREDENTIALS"), "原因码 BAD_CREDENTIALS", bad.body());

        HttpResponse<String> malformed = telemetry("http-dev", "secret-h", "not-json");
        assertEquals(400, malformed.statusCode(), "非 JSON -> 400");
        assertTrue(malformed.body().contains("MALFORMED_PAYLOAD"), "原因码 MALFORMED_PAYLOAD", malformed.body());

        HttpResponse<String> noToken = http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/admin/devices/http-dev/shadow")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(401, noToken.statusCode(), "北向缺少 token -> 401");
        assertTrue(noToken.body().contains("UNAUTHORIZED"), "原因码 UNAUTHORIZED", noToken.body());
    }

    @Test
    @Order(2)
    void telemetryNormalizationAndStaleProtection() throws Exception {
        log.info("==== 场景：HTTP 遥测归一化、缺省语义与乱序保护 ====");
        assertEquals(200, telemetry("http-dev", "secret-h",
                "{\"seq\":3,\"ts\":300,\"properties\":{\"temp\":30}}").statusCode(), "seq=3 接受");
        assertEquals(200, telemetry("http-dev", "secret-h",
                "{\"seq\":2,\"ts\":200,\"properties\":{\"temp\":20}}").statusCode(), "seq=2 迟到不报错(被忽略)");

        JsonNode shadow = JsonSupport.mapper().readTree(http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/admin/devices/http-dev/shadow"))
                        .header("X-Admin-Token", "admin-token").GET().build(),
                HttpResponse.BodyHandlers.ofString()).body());
        assertEquals(30, shadow.get("reported").get("temp").asInt(), "影子保持 seq=3 的 temp=30");
        HttpResponse<String> stale = telemetry("http-dev", "secret-h",
                "{\"seq\":1,\"ts\":100,\"properties\":{\"temp\":10}}");
        assertEquals("STALE_TELEMETRY_IGNORED",
                JsonSupport.mapper().readTree(stale.body()).get("reason").asText(),
                "迟到遥测响应 reason=STALE_TELEMETRY_IGNORED");

        // 缺省：不带 ts/seq/properties
        HttpResponse<String> defaults = telemetry("http-dev", "secret-h", "{}");
        assertEquals(200, defaults.statusCode(), "全缺省仍接受");
        JsonNode body = JsonSupport.mapper().readTree(defaults.body());
        assertEquals(4L, body.get("reportedSeq").asLong(), "缺省 seq 派生为 4");
    }

    @Test
    @Order(3)
    void offlineCommandsBackfilledOverHttpAndAcked() throws Exception {
        log.info("==== 场景：HTTP 离线命令补齐、确认与生命周期查询 ====");
        // 设备未开始轮询 => 离线
        HttpResponse<String> send1 = http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/admin/devices/http-cmd/commands"))
                        .header("X-Admin-Token", "admin-token")
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"name\":\"set-fan\",\"params\":{\"speed\":5},\"desired\":{\"fan\":5}}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(202, send1.statusCode(), "离线命令被接受入队");
        String commandId = JsonSupport.mapper().readTree(send1.body()).get("commandId").asText();
        assertEquals("QUEUED", JsonSupport.mapper().readTree(send1.body()).get("status").asText(),
                "状态为 QUEUED");

        HttpDeviceSimulator device = new HttpDeviceSimulator(base, "http-cmd", "secret-cmd");
        device.connect(); // 上线长轮询，自动 ack
        waitFor("命令通过长轮询补齐并 ACKED", Duration.ofSeconds(6),
                () -> {
                    try {
                        HttpResponse<String> q = http.send(HttpRequest.newBuilder(
                                        URI.create(base + "/api/admin/devices/http-cmd/commands/" + commandId))
                                        .header("X-Admin-Token", "admin-token").GET().build(),
                                HttpResponse.BodyHandlers.ofString());
                        return JsonSupport.mapper().readTree(q.body()).get("status").asText()
                                .equals(CommandStatus.ACKED.name());
                    } catch (Exception e) {
                        return false;
                    }
                });

        HttpResponse<String> historyResp = http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/admin/devices/http-cmd/commands"))
                        .header("X-Admin-Token", "admin-token").GET().build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode history = JsonSupport.mapper().readTree(historyResp.body()).get("commands");
        assertEquals("ACKED", history.get(0).get("status").asText(), "命令历史可查询确认结果");
        assertEquals(0, history.get(0).get("ackResult").get("code").asInt(), "ack 结果内容可查询");
        device.disconnect();
    }

    @Test
    @Order(4)
    void shadowSyncStatesTransitionCorrectly() throws Exception {
        log.info("==== 场景：影子期望/实际与同步状态转换 ====");
        register("shadow-dev", "secret-s");
        http.send(HttpRequest.newBuilder(URI.create(base + "/api/admin/devices/shadow-dev/desired"))
                .header("X-Admin-Token", "admin-token")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"desired\":{\"mode\":\"eco\"}}")).build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode outOfSync = JsonSupport.mapper().readTree(http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/admin/devices/shadow-dev/shadow"))
                        .header("X-Admin-Token", "admin-token").GET().build(),
                HttpResponse.BodyHandlers.ofString()).body());
        assertEquals("OUT_OF_SYNC", outOfSync.get("syncState").asText(), "有 delta => OUT_OF_SYNC");
        assertTrue(outOfSync.get("delta").has("mode"), "delta 明确列出差异键", outOfSync.get("delta").toString());

        telemetry("shadow-dev", "secret-s",
                "{\"seq\":1,\"ts\":10,\"properties\":{\"mode\":\"eco\"}}");
        JsonNode inSync = JsonSupport.mapper().readTree(http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/admin/devices/shadow-dev/shadow"))
                        .header("X-Admin-Token", "admin-token").GET().build(),
                HttpResponse.BodyHandlers.ofString()).body());
        assertEquals("IN_SYNC", inSync.get("syncState").asText(), "实际值追平期望 => IN_SYNC");
    }

    @Test
    @Order(5)
    void concurrentHttpWritesAreConsistent() throws Exception {
        log.info("==== 场景：HTTP 并发上报无丢更新 ====");
        register("http-concurrent", "secret-x");
        int threads = 10;
        int n = 30;
        java.util.List<Thread> pool = new java.util.ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int base = t;
            pool.add(new Thread(() -> {
                for (int i = 0; i < n; i++) {
                    long seq = (long) base * n + i + 1;
                    try {
                        telemetry("http-concurrent", "secret-x",
                                "{\"seq\":" + seq + ",\"ts\":" + (seq * 10)
                                        + ",\"properties\":{\"shared\":" + seq + "}}");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }));
        }
        pool.forEach(Thread::start);
        for (Thread t : pool) {
            t.join(15_000);
        }
        JsonNode shadow = JsonSupport.mapper().readTree(http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/admin/devices/http-concurrent/shadow"))
                        .header("X-Admin-Token", "admin-token").GET().build(),
                HttpResponse.BodyHandlers.ofString()).body());
        assertEquals((long) threads * n, shadow.get("reportedSeq").asLong(), "全部序号写入，无丢失");
        assertEquals((long) threads * n * 10, shadow.get("reportedTs").asLong(), "最新时间戳正确");
        assertEquals((long) threads * n, shadow.get("reported").get("shared").asLong(),
                "shared 为最大值，状态未回退");
    }
}
