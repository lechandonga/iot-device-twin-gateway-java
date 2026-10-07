package com.github.lechandonga.iotdevicetwingatewayjava.simulator;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.lechandonga.iotdevicetwingatewayjava.core.DeviceSession;
import com.github.lechandonga.iotdevicetwingatewayjava.http.JsonSupport;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.EmbeddedMqttBroker;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.MqttGatewayAdapter;
import com.github.lechandonga.iotdevicetwingatewayjava.mqtt.MqttRejectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 基于内嵌 Broker 的 MQTT 设备客户端：模拟真实设备的连接、遗嘱离线、重连、
 * 遥测发布、命令订阅与确认。收到命令后默认自动 ack（可替换处理器模拟不响应导致超时）。
 */
public class MqttDeviceSimulator implements DeviceSession {

    private static final Logger log = LoggerFactory.getLogger(MqttDeviceSimulator.class);

    private final EmbeddedMqttBroker broker;
    private final MqttGatewayAdapter adapter;
    private final String deviceId;
    private final String secret;
    private Consumer<Command> commandHandler;
    private String clientId;
    private boolean connected;

    private final CopyOnWriteArrayList<Command> receivedCommands = new CopyOnWriteArrayList<>();
    private final Map<String, Boolean> acked = new ConcurrentHashMap<>();
    private final java.util.concurrent.ExecutorService ackExecutor;

    public MqttDeviceSimulator(EmbeddedMqttBroker broker, MqttGatewayAdapter adapter,
                               String deviceId, String secret) {
        this.broker = broker;
        this.adapter = adapter;
        this.deviceId = deviceId;
        this.secret = secret;
        this.commandHandler = this::autoAck;
        this.ackExecutor = java.util.concurrent.Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "mqtt-device-ack-" + deviceId);
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public ProtocolType protocol() {
        return ProtocolType.MQTT;
    }

    public String deviceId() {
        return deviceId;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setCommandHandler(Consumer<Command> handler) {
        this.commandHandler = handler;
    }

    /** 建立连接（重连时再次调用即可，遗嘱会重新注册）。 */
    public void connect() {
        this.clientId = adapter.deviceAttach(deviceId, secret, this::onCommandMessage);
        connected = true; // 先置在线，再触发命令补齐，否则首批命令的 ack 会被当作离线丢弃
        adapter.deviceOnline(deviceId, secret, clientId, this);
        log.info("[设备:{}] MQTT 已连接(在线)", deviceId);
    }

    /** @param graceful true 主动断连；false 模拟崩溃/掉电，Broker 触发遗嘱 offline */
    public void disconnect(boolean graceful) {
        if (!connected) {
            return;
        }
        connected = false;
        adapter.deviceDisconnect(clientId, deviceId, graceful);
        log.info("[设备:{}] MQTT {} (离线)", deviceId, graceful ? "主动断连" : "异常断连，遗嘱发布 offline");
    }

    public void publishTelemetry(Map<String, Object> telemetryJson) {
        if (!connected) {
            throw new IllegalStateException("device offline: " + deviceId);
        }
        Map<String, Object> body = new HashMap<>(telemetryJson);
        body.putIfAbsent("deviceId", deviceId);
        body.put("secret", secret);
        try {
            broker.publish(clientId, "devices/" + deviceId + "/telemetry",
                    JsonSupport.mapper().writeValueAsBytes(body), false);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 直接发布原始字节，用于畸形报文测试。 */
    public void publishRawTelemetry(byte[] raw) {
        broker.publish(clientId, "devices/" + deviceId + "/telemetry", raw, false);
    }

    private void onCommandMessage(String topic, byte[] payload) {
        try {
            JsonNode json = JsonSupport.mapper().readTree(payload);
            @SuppressWarnings("unchecked")
            Map<String, Object> params = json.has("params")
                    ? JsonSupport.mapper().convertValue(json.get("params"), Map.class) : Map.of();
            Command view = new Command(json.get("commandId").asText(), deviceId,
                    json.get("name").asText(), params, null, 0, 0, null, null, null, null, null);
            receivedCommands.add(view);
            log.info("[设备:{}] 收到命令 commandId={} name={}", deviceId, view.commandId(), view.name());
            // 真实设备不会在收包调用栈内同步应答；异步处理避免与 Broker fanout 嵌套
            ackExecutor.execute(() -> commandHandler.accept(view));
        } catch (Exception e) {
            log.warn("[设备:{}] 命令处理失败: {}", deviceId, e.toString(), e);
        }
    }

    /** DeviceSession 回调：核心层发布命令到 Broker，Broker 再推给本设备。 */
    @Override
    public void deliver(Command command) {
        adapter.publishCommand(command);
    }

    public void autoAck(Command command) {
        ack(command, Map.of("code", 0, "msg", "done"), Map.of());
    }

    public void ack(Command command, Map<String, Object> result, Map<String, Object> reported) {
        if (!connected) {
            return;
        }
        acked.put(command.commandId(), true);
        Map<String, Object> body = Map.of(
                "secret", secret,
                "commandId", command.commandId(),
                "result", result == null ? Map.of() : result,
                "reported", reported == null ? Map.of() : reported);
        try {
            broker.publish(clientId, "devices/" + deviceId + "/cmd/ack",
                    JsonSupport.mapper().writeValueAsBytes(body), false);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public CopyOnWriteArrayList<Command> receivedCommands() {
        return receivedCommands;
    }

    public Map<String, Boolean> ackedCommands() {
        return acked;
    }
}
