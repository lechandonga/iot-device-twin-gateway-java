package com.github.lechandonga.iotdevicetwingatewayjava.simulate;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * HTTP 设备模拟器：连接、遥测上报、命令长轮询与 ACK，全部使用 JDK HttpClient。
 */
public class HttpDeviceSimulator implements AutoCloseable {

    private final String baseUrl;
    private final String deviceId;
    private final String secret;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile boolean running;
    private Thread pollThread;

    public HttpDeviceSimulator(String baseUrl, String deviceId, String secret) {
        this.baseUrl = baseUrl;
        this.deviceId = deviceId;
        this.secret = secret;
    }

    public void connect() throws Exception {
        post("/connect", null);
    }

    public void disconnect() throws Exception {
        post("/disconnect", null);
    }

    public HttpResponse<String> post(String path, Object body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/devices/" + deviceId + path))
                .header("X-Device-Id", deviceId)
                .header("X-Device-Secret", secret)
                .timeout(Duration.ofSeconds(5));
        if (body == null) {
            builder.POST(HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body)));
        }
        HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        return response;
    }

    public HttpResponse<String> rawTelemetry(String rawJson) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/devices/" + deviceId + "/telemetry"))
                .header("X-Device-Id", deviceId)
                .header("X-Device-Secret", secret)
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(rawJson))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public void reportTelemetry(long ts, Map<String, Object> properties) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ts", ts);
        body.put("properties", properties);
        post("/telemetry", body);
    }

    public void startPolling() {
        running = true;
        pollThread = new Thread(() -> {
            while (running) {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/api/v1/devices/" + deviceId
                                    + "/commands/pull?waitSeconds=2"))
                            .header("X-Device-Id", deviceId)
                            .header("X-Device-Secret", secret)
                            .timeout(Duration.ofSeconds(4))
                            .GET()
                            .build();
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        var node = objectMapper.readTree(response.body());
                        String commandId = node.path("commandId").asText();
                        post("/commands/" + commandId + "/ack", Map.of("result", "EXECUTED"));
                    }
                } catch (Exception e) {
                    if (running) {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
        }, "http-device-poll-" + deviceId);
        pollThread.setDaemon(true);
        pollThread.start();
    }

    @Override
    public void close() {
        running = false;
        if (pollThread != null) {
            pollThread.interrupt();
        }
    }
}
