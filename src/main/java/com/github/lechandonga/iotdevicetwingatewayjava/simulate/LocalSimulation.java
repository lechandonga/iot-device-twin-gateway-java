package com.github.lechandonga.iotdevicetwingatewayjava.simulate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.MqttClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 本地进程模拟演示（独立 main，不依赖 Spring）：
 * 需要先启动 Application（网关），再运行本类。
 * 演示：MQTT/HTTP 设备上线、遥测、离线命令排队、重连补齐、ACK。
 */
public final class LocalSimulation {

    private static final Logger log = LoggerFactory.getLogger(LocalSimulation.class);

    private LocalSimulation() {
    }

    public static void main(String[] args) throws Exception {
        String baseUrl = "http://localhost:8080";
        int mqttPort = 1883;
        HttpClient http = HttpClient.newHttpClient();
        ObjectMapper mapper = new ObjectMapper();

        cloudRegister(http, mapper, baseUrl, "sensor-mqtt", "secret-mqtt");
        cloudRegister(http, mapper, baseUrl, "sensor-http", "secret-http");

        MqttClient mqttDevice = new MqttClient("127.0.0.1", mqttPort,
                "sensor-mqtt", "sensor-mqtt", "secret-mqtt",
                "devices/sensor-mqtt/status", "OFFLINE");
        String connectError = mqttDevice.connect();
        if (connectError != null) {
            log.error("MQTT 设备连接失败: {}", connectError);
            return;
        }
        mqttDevice.subscribe("devices/sensor-mqtt/cmd", (topic, payload) -> {
            try {
                var node = mapper.readTree(payload);
                String commandId = node.path("commandId").asText();
                mqttDevice.publishQos1("devices/sensor-mqtt/cmd/ack",
                        mapper.writeValueAsBytes(Map.of("commandId", commandId, "result", "EXECUTED")));
                log.info("模拟设备(MQTT) 执行并确认命令 {}", commandId);
            } catch (Exception e) {
                log.error("命令处理失败", e);
            }
        });

        mqttDevice.publishQos1("devices/sensor-mqtt/telemetry",
                mapper.writeValueAsBytes(Map.of("ts", System.currentTimeMillis(),
                        "properties", Map.of("temp", 26.1))));

        HttpDeviceSimulator httpDevice = new HttpDeviceSimulator(baseUrl, "sensor-http", "secret-http");
        httpDevice.connect();
        httpDevice.reportTelemetry(System.currentTimeMillis(), Map.of("humidity", 55));
        httpDevice.startPolling();

        log.info("== 场景：设备离线期间下发命令，验证不丢弃、重连补齐 ==");
        mqttDevice.close();
        Thread.sleep(300);
        cloudIssue(http, mapper, baseUrl, "sensor-mqtt", "MQTT", "reboot", Map.of("delay", 1));
        cloudIssue(http, mapper, baseUrl, "sensor-mqtt", "MQTT", "calibrate", Map.of("offset", 2));
        log.info("离线期间已下发 2 条命令，5 秒后设备重连……");
        Thread.sleep(5000);

        MqttClient reconnected = new MqttClient("127.0.0.1", mqttPort,
                "sensor-mqtt", "sensor-mqtt", "secret-mqtt",
                "devices/sensor-mqtt/status", "OFFLINE");
        reconnected.connect();
        reconnected.subscribe("devices/sensor-mqtt/cmd", (topic, payload) -> {
            try {
                var node = mapper.readTree(payload);
                String commandId = node.path("commandId").asText();
                reconnected.publishQos1("devices/sensor-mqtt/cmd/ack",
                        mapper.writeValueAsBytes(Map.of("commandId", commandId, "result", "EXECUTED")));
                log.info("重连设备(MQTT) 按序执行并确认命令 {}", commandId);
            } catch (Exception e) {
                log.error("命令处理失败", e);
            }
        });

        cloudIssue(http, mapper, baseUrl, "sensor-http", "HTTP", "set-led", Map.of("on", true));

        Thread.sleep(4000);
        log.info("命令事件流水：");
        cloudPrintEvents(http, baseUrl);
        reconnected.close();
        httpDevice.close();
        System.exit(0);
    }

    private static void cloudRegister(HttpClient http, ObjectMapper mapper, String baseUrl,
                                      String deviceId, String secret) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/cloud/devices"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        mapper.writeValueAsString(Map.of("deviceId", deviceId, "secret", secret))))
                .build();
        http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void cloudIssue(HttpClient http, ObjectMapper mapper, String baseUrl,
                                   String deviceId, String transport, String name,
                                   Map<String, Object> payload) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("payload", payload);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/cloud/devices/" + deviceId
                        + "/commands?transport=" + transport))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        log.info("下发 {} 命令到 {} -> {}", name, deviceId, response.body());
    }

    private static void cloudPrintEvents(HttpClient http, String baseUrl) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/cloud/commands/events"))
                .GET()
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        log.info("\n{}", response.body());
    }
}
