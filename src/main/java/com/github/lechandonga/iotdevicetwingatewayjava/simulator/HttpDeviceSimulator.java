package com.github.lechandonga.iotdevicetwingatewayjava.simulator;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.lechandonga.iotdevicetwingatewayjava.http.JsonSupport;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * HTTP 接入的设备进程模拟器：后台线程持续长轮询 /commands（=在线），
 * 停止轮询并发送 DELETE（=主动离线）；重新启动轮询线程（=重连后补齐命令）。
 */
public class HttpDeviceSimulator {

    private static final Logger log = LoggerFactory.getLogger(HttpDeviceSimulator.class);

    private final String baseUrl;
    private final String deviceId;
    private final String secret;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private ScheduledExecutorService poller;
    private Consumer<Command> commandHandler;
    private final List<Command> received = new ArrayList<>();

    public HttpDeviceSimulator(String baseUrl, String deviceId, String secret) {
        this.baseUrl = baseUrl;
        this.deviceId = deviceId;
        this.secret = secret;
        this.commandHandler = this::autoAck;
    }

    public void setCommandHandler(Consumer<Command> handler) {
        this.commandHandler = handler;
    }

    /** 启动长轮询，即“上线”。重连时再次调用即可。 */
    public synchronized void connect() {
        if (poller != null && !poller.isShutdown()) {
            return;
        }
        poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "http-device-" + deviceId);
            t.setDaemon(true);
            return t;
        });
        poller.submit(this::pollLoop);
        log.info("[设备:{}] HTTP 长轮询已启动(在线)", deviceId);
    }

    private void pollLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                HttpResponse<String> resp = http.send(
                        HttpRequest.newBuilder(URI.create(baseUrl + "/api/device/" + deviceId
                                + "/commands?waitMs=500"))
                                .header("X-Device-Secret", secret).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() != 200) {
                    log.warn("[设备:{}] 轮询被拒 status={} body={}", deviceId, resp.statusCode(), resp.body());
                    Thread.sleep(300);
                    continue;
                }
                JsonNode commands = JsonSupport.mapper().readTree(resp.body()).get("commands");
                if (commands != null) {
                    for (JsonNode c : commands) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> params = c.has("params")
                                ? JsonSupport.mapper().convertValue(c.get("params"), Map.class) : Map.of();
                        Command cmd = new Command(c.get("commandId").asText(), deviceId,
                                c.get("name").asText(), params, null, 0, 0, null, null, null, null, null);
                        synchronized (this) {
                            received.add(cmd);
                        }
                        log.info("[设备:{}] 收到命令 commandId={} name={}", deviceId, cmd.commandId(), cmd.name());
                        commandHandler.accept(cmd);
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException ie) {
                    return;
                }
            }
        }
    }

    public void disconnect() {
        if (poller == null) {
            return;
        }
        poller.shutdownNow();
        poller = null;
        try {
            http.send(HttpRequest.newBuilder(URI.create(baseUrl + "/api/device/" + deviceId + "/commands"))
                    .header("X-Device-Secret", secret).DELETE().build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception ignored) {
        }
        log.info("[设备:{}] HTTP 停止轮询(离线)", deviceId);
    }

    public HttpResponse<String> publishTelemetry(Map<String, Object> properties, Long ts, Long seq) throws Exception {
        Map<String, Object> body = new HashMap<>();
        if (properties != null) {
            body.put("properties", properties);
        }
        if (ts != null) {
            body.put("ts", ts);
        }
        if (seq != null) {
            body.put("seq", seq);
        }
        return http.send(HttpRequest.newBuilder(
                        URI.create(baseUrl + "/api/device/" + deviceId + "/telemetry"))
                        .header("X-Device-Secret", secret)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(JsonSupport.mapper().writeValueAsString(body)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> publishRaw(String rawJson) throws Exception {
        return http.send(HttpRequest.newBuilder(
                        URI.create(baseUrl + "/api/device/" + deviceId + "/telemetry"))
                        .header("X-Device-Secret", secret)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(rawJson))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    public void autoAck(Command cmd) {
        try {
            ack(cmd.commandId(), Map.of("code", 0, "msg", "done"), Map.of());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HttpResponse<String> ack(String commandId, Map<String, Object> result,
                                    Map<String, Object> reported) throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put("commandId", commandId);
        body.put("result", result);
        body.put("reported", reported);
        return http.send(HttpRequest.newBuilder(URI.create(baseUrl + "/api/device/"
                        + deviceId + "/commands/" + commandId + "/ack"))
                        .header("X-Device-Secret", secret)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(JsonSupport.mapper().writeValueAsString(body)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    public synchronized List<Command> receivedCommands() {
        return List.copyOf(received);
    }
}
