package com.github.lechandonga.iotdevicetwingatewayjava.demo;

import com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayCore;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.CommandStatus;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceShadow;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.MqttGatewayAdapter;
import com.github.lechandonga.iotdevicetwingatewayjava.simulator.HttpDeviceSimulator;
import com.github.lechandonga.iotdevicetwingatewayjava.simulator.MqttDeviceSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 本地业务场景演示（不依赖外部 Broker/中间件/真实设备）：
 * 启动方式见 README，运行日志会逐步打印在线、离线、重连补齐、超时重试、并发上报等场景。
 */
@Component
@Profile("demo")
@Order(0)
public class LocalDemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger("本地演示");

    private final GatewayCore core;
    private final MqttGatewayAdapter mqttAdapter;
    private final Environment environment;

    public LocalDemoRunner(GatewayCore core, MqttGatewayAdapter mqttAdapter, Environment environment) {
        this.core = core;
        this.mqttAdapter = mqttAdapter;
        this.environment = environment;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("================ 物联网设备接入网关：本地场景演示 ================");

        core.register("demo-mqtt", "secret-mqtt");
        core.register("demo-http", "secret-http");

        // 场景 1：MQTT 设备离线期间命令排队，重连后按序补齐
        log.info("---- 场景1：MQTT 离线命令入队 -> 上线 -> 按序补齐并确认 ----");
        Command a = core.sendCommand("demo-mqtt", "set-led", Map.of("color", "red"), Map.of("led", "red"));
        Command b = core.sendCommand("demo-mqtt", "set-speed", Map.of("rpm", 200), null);
        log.info("离线时命令状态: {} {}", a.commandId().substring(0, 8), a.status());
        log.info("离线时命令状态: {} {}", b.commandId().substring(0, 8), b.status());

        MqttDeviceSimulator mqttDevice =
                new MqttDeviceSimulator(mqttAdapter.broker(), mqttAdapter, "demo-mqtt", "secret-mqtt");
        mqttDevice.connect();
        mqttDevice.publishTelemetry(Map.of("seq", 1L, "ts", 1000L, "properties", Map.of("temp", 25)));
        Thread.sleep(800);
        List<Command> history = core.commandHistory("demo-mqtt");
        log.info("补齐后命令状态: {}", history.stream().map(c -> c.name() + "=" + c.status()).toList());

        // 场景 2：乱序遥测被旧数据保护忽略
        log.info("---- 场景2：乱序遥测不覆盖新鲜状态 ----");
        mqttDevice.publishTelemetry(Map.of("seq", 3L, "ts", 3000L, "properties", Map.of("temp", 30)));
        mqttDevice.publishTelemetry(Map.of("seq", 2L, "ts", 2000L, "properties", Map.of("temp", 20)));
        DeviceShadow shadow = core.shadow("demo-mqtt");
        log.info("影子 reported={} seq={} temp 保持最新值 30", shadow.reported(), shadow.reportedSeq());

        // 场景 3：异常掉线（MQTT 遗嘱）-> 重连
        log.info("---- 场景3：异常掉线触发遗嘱 offline，随后重连 ----");
        mqttDevice.disconnect(false);
        Thread.sleep(300);
        log.info("掉线后连接状态: {}", core.shadow("demo-mqtt").connectionState());
        mqttDevice.connect();
        Thread.sleep(200);
        log.info("重连后连接状态: {}", core.shadow("demo-mqtt").connectionState());

        // 场景 4：HTTP 设备长轮询接收命令
        log.info("---- 场景4：HTTP 设备长轮询上线 -> 命令下发 -> 确认 ----");
        Command h = core.sendCommand("demo-http", "reboot", Map.of("delay", 5), null);
        int port = Integer.parseInt(environment.getProperty("server.port", "8080"));
        HttpDeviceSimulator httpDevice = new HttpDeviceSimulator(
                "http://localhost:" + port, "demo-http", "secret-http");
        httpDevice.connect();
        Thread.sleep(1500);
        Command hq = core.command("demo-http", h.commandId()).orElseThrow();
        log.info("HTTP 命令状态: {} attempts={}", hq.status(), hq.attempts());
        httpDevice.publishTelemetry(Map.of(), 9000L, 1L);

        // 场景 5：影子同步状态
        log.info("---- 场景5：影子 desired/reported 与同步状态 ----");
        core.updateDesired("demo-http", Map.of("mode", "eco"));
        httpDevice.publishTelemetry(Map.of("mode", "normal"), 9100L, 2L);
        log.info("HTTP 影子: {}", core.shadow("demo-http"));

        log.info("================ 演示结束，网关继续运行；Ctrl+C 退出 ================");
    }
}
