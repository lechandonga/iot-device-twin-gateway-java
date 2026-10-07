package com.github.lechandonga.iotdevicetwingatewayjava.tests;

import com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayCore;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.CommandStatus;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ConnectionState;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceShadow;
import com.github.lechandonga.iotdevicetwingatewayjava.model.SyncState;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.EmbeddedMqttBroker;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.MqttGatewayAdapter;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.MqttRejectException;
import com.github.lechandonga.iotdevicetwingatewayjava.simulator.MqttDeviceSimulator;
import org.junit.jupiter.api.AfterEach;
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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.assertEquals;
import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.assertTrue;
import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.waitFor;

/**
 * MQTT 接入业务场景测试：鉴权拒绝、乱序遥测、离线命令补齐、超时重试、并发写入。
 * 运行时日志会逐条打印 [场景] 与 [通过]/[失败]，直接体现覆盖情况。
 */
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MqttScenarioTests {

    private static final Logger log = LoggerFactory.getLogger("MQTT场景");

    @Autowired
    private GatewayCore core;

    private EmbeddedMqttBroker broker;
    private MqttGatewayAdapter adapter;

    @BeforeAll
    void setup() {
        adapter = new MqttGatewayAdapter(core);
        broker = adapter.broker();
        core.register("sensor-a", "secret-a");
        core.register("sensor-b", "secret-b");
        core.register("slow-device", "secret-slow");
    }

    @AfterEach
    void cleanup() {
        // 状态保留以便跨用例演示；这里仅记录
    }

    @Test
    @Order(1)
    void authRejectionsAreDistinguishable() {
        log.info("==== 场景：MQTT 鉴权拒绝（未注册 / 凭据错误） ====");

        MqttDeviceSimulator unknown = new MqttDeviceSimulator(broker, adapter, "ghost", "x");
        try {
            unknown.connect();
            throw new AssertionError("未注册设备应被拒绝");
        } catch (MqttRejectException e) {
            assertEquals("DEVICE_NOT_REGISTERED", e.reasonCode(), "未注册设备原因可区分");
        }

        MqttDeviceSimulator badSecret = new MqttDeviceSimulator(broker, adapter, "sensor-a", "wrong");
        try {
            badSecret.connect();
            throw new AssertionError("凭据错误设备应被拒绝");
        } catch (MqttRejectException e) {
            assertEquals("BAD_CREDENTIALS", e.reasonCode(), "凭据错误原因可区分");
        }
    }

    @Test
    @Order(2)
    void malformedPayloadIsRejected() {
        log.info("==== 场景：MQTT 畸形报文拒绝 ====");
        MqttDeviceSimulator device = new MqttDeviceSimulator(broker, adapter, "sensor-a", "secret-a");
        device.connect();
        try {
            device.publishRawTelemetry("not-json{".getBytes());
            throw new AssertionError("畸形报文应被拒绝");
        } catch (MqttRejectException e) {
            assertEquals("MALFORMED_PAYLOAD", e.reasonCode(), "非 JSON 报 MALFORMED_PAYLOAD");
        }

        try {
            device.publishRawTelemetry(
                    "{\"secret\":\"secret-a\",\"properties\":{\"temp\":1},\"deviceId\":\"other\"}".getBytes());
            throw new AssertionError("deviceId 与主题不一致应被拒绝");
        } catch (MqttRejectException e) {
            assertEquals("MALFORMED_PAYLOAD", e.reasonCode(), "deviceId 不一致报 MALFORMED_PAYLOAD");
        }
        device.disconnect(true);
    }

    @Test
    @Order(3)
    void outOfOrderTelemetryDoesNotRollBackShadow() {
        log.info("==== 场景：乱序/迟到遥测不得覆盖更新鲜状态 ====");
        MqttDeviceSimulator device = new MqttDeviceSimulator(broker, adapter, "sensor-a", "secret-a");
        device.connect();

        device.publishTelemetry(Map.of("seq", 10L, "ts", 1000L, "properties", Map.of("temp", 25)));
        device.publishTelemetry(Map.of("seq", 12L, "ts", 1200L, "properties", Map.of("temp", 27)));
        device.publishTelemetry(Map.of("seq", 11L, "ts", 1100L, "properties", Map.of("temp", 26)));
        device.publishTelemetry(Map.of("seq", 12L, "ts", 1100L, "properties", Map.of("temp", 20)));

        DeviceShadow shadow = core.shadow("sensor-a");
        assertEquals(27, shadow.reported().get("temp"), "乱序与同序旧时间戳均被忽略，影子保持最新 27");
        assertEquals(12L, shadow.reportedSeq(), "reportedSeq 保持 12");
        assertEquals(1200L, shadow.reportedTs(), "reportedTs 保持 1200");

        // 缺省语义：无 seq/ts 时 seq 派生为 13，ts 取接收时刻 => 可被接受
        device.publishTelemetry(Map.of("properties", Map.of("humidity", 60)));
        DeviceShadow afterDefault = core.shadow("sensor-a");
        assertEquals(13L, afterDefault.reportedSeq(), "缺失 seq 稳定派生为最大序号+1");
        assertEquals(60, afterDefault.reported().get("humidity"), "缺省 properties 语义下正常字段写入");
        device.disconnect(true);
    }

    @Test
    @Order(4)
    void offlineCommandsAreBackfilledInOrderAfterReconnect() throws Exception {
        log.info("==== 场景：离线命令不丢失，重连按原顺序补齐并确认 ====");
        MqttDeviceSimulator device = new MqttDeviceSimulator(broker, adapter, "sensor-b", "secret-b");

        // 未连接时下发三条命令
        Command c1 = core.sendCommand("sensor-b", "set-led", Map.of("on", true), Map.of("led", true));
        Command c2 = core.sendCommand("sensor-b", "set-speed", Map.of("rpm", 100), null);
        Command c3 = core.sendCommand("sensor-b", "reboot", Map.of(), null);
        assertEquals(CommandStatus.QUEUED, c1.status(), "离线命令进入 QUEUED 而非丢弃");
        assertEquals(SyncState.OUT_OF_SYNC, core.shadow("sensor-b").syncState(),
                "存在期望未达成 => OUT_OF_SYNC");

        device.connect();
        waitFor("三条命令按序到达并 ack", Duration.ofSeconds(5), () ->
                device.receivedCommands().size() == 3 && device.ackedCommands().size() == 3);

        List<Command> received = device.receivedCommands();
        assertEquals(c1.commandId(), received.get(0).commandId(), "第 1 条命令顺序一致");
        assertEquals(c2.commandId(), received.get(1).commandId(), "第 2 条命令顺序一致");
        assertEquals(c3.commandId(), received.get(2).commandId(), "第 3 条命令顺序一致");

        List<Command> history = core.commandHistory("sensor-b");
        assertTrue(history.stream().allMatch(c -> c.status() == CommandStatus.ACKED),
                "命令全部 ACKED", "状态=" + history.stream().map(Command::status).toList());
        device.disconnect(true);
    }

    @Test
    @Order(5)
    void commandTimeoutAndRetryAreQueryable() throws Exception {
        log.info("==== 场景：命令超时、重试、最终结果可查询 ====");
        MqttDeviceSimulator device = new MqttDeviceSimulator(broker, adapter, "slow-device", "secret-slow");
        // 设备永远不 ack
        device.setCommandHandler(cmd -> log.info("slow-device 收到命令但不确认: {}", cmd.commandId()));
        device.connect();

        Command sent = core.sendCommand("slow-device", "dangerous-op", Map.of(), null);
        waitFor("命令重试至最大次数后 FAILED", Duration.ofSeconds(10), () ->
                core.command("slow-device", sent.commandId())
                        .map(c -> c.status() == CommandStatus.FAILED).orElse(false));

        Command failed = core.command("slow-device", sent.commandId()).orElseThrow();
        assertEquals(failed.maxAttempts(), failed.attempts(), "尝试次数达到上限");
        assertTrue(failed.lastError().contains("timeout"), "失败原因明确为 ack 超时", failed.lastError());
        device.disconnect(false); // 异常断连触发遗嘱
        waitFor("遗嘱使设备离线", Duration.ofSeconds(3),
                () -> core.shadow("slow-device").connectionState() == ConnectionState.OFFLINE);
        assertEquals(ConnectionState.OFFLINE, core.shadow("slow-device").connectionState(),
                "MQTT 遗嘱 offline 被网关注解为离线");
    }

    @Test
    @Order(6)
    void concurrentWritesDoNotLoseUpdates() throws Exception {
        log.info("==== 场景：同一设备并发遥测+期望写入不丢更新、影子不回退 ====");
        core.register("concurrent-dev", "secret-c");
        MqttDeviceSimulator device =
                new MqttDeviceSimulator(broker, adapter, "concurrent-dev", "secret-c");
        device.connect();

        int threads = 12;
        int perThread = 40;
        List<Thread> workers = new java.util.ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int base = t;
            workers.add(new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    long seq = (long) base * perThread + i + 1;
                    device.publishTelemetry(Map.of(
                            "seq", seq,
                            "ts", seq * 10L,
                            "properties", Map.of("k" + base, i, "shared", seq)));
                    if (i % 10 == 0) {
                        core.updateDesired("concurrent-dev", Map.of("d" + base, i));
                    }
                }
            }));
        }
        workers.forEach(Thread::start);
        for (Thread w : workers) {
            w.join(15_000);
        }

        DeviceShadow shadow = core.shadow("concurrent-dev");
        long expectedSeq = (long) threads * perThread;
        assertEquals(expectedSeq, shadow.reportedSeq(), "全部遥测序号被观测，无丢更新");
        assertEquals(expectedSeq * 10L, shadow.reportedTs(), "最新时间戳正确");
        assertEquals(expectedSeq, ((Number) shadow.reported().get("shared")).longValue(),
                "shared 字段为最大序号值，影子未回退");
        for (int t = 0; t < threads; t++) {
            assertTrue(shadow.desired().containsKey("k" + t) || shadow.desired().containsKey("d" + t),
                    "并发期望写入保留", "设备 k" + t + "/d" + t + " 可查");
        }
        assertTrue(shadow.desired().size() >= threads, "所有并发期望键均存在",
                "desired.size=" + shadow.desired().size());
        device.disconnect(true);
    }

}
