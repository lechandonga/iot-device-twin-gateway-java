package com.github.lechandonga.iotdevicetwingatewayjava.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.lechandonga.iotdevicetwingatewayjava.core.DeviceSession;
import com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayCore;
import com.github.lechandonga.iotdevicetwingatewayjava.core.TelemetryIngest;
import com.github.lechandonga.iotdevicetwingatewayjava.http.JsonSupport;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.GatewayException;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * MQTT 接入适配层。主题映射：
 * <pre>
 * devices/{deviceId}/telemetry   上行遥测（设备发布，网关订阅）
 * devices/{deviceId}/cmd/ack     上行命令确认（设备发布，网关订阅）
 * devices/{deviceId}/status      在线状态（retained，网关发布 online/offline）
 * devices/{deviceId}/commands/#  下行命令（网关发布，设备订阅）
 * </pre>
 * 设备 CONNECT 时 username=deviceId、password=secret；未注册/凭据错误在 CONNECT 阶段拒绝。
 */
@Component
public class MqttGatewayAdapter {

    private static final Logger log = LoggerFactory.getLogger(MqttGatewayAdapter.class);

    private final GatewayCore core;
    private final EmbeddedMqttBroker broker;

    public MqttGatewayAdapter(GatewayCore core) {
        this.core = core;
        this.broker = new EmbeddedMqttBroker(this::authenticateConnect);
        // 网关订阅所有设备的上行主题（通配符）
        broker.subscribe("gateway", "devices/+/telemetry", this::onTelemetry);
        broker.subscribe("gateway", "devices/+/cmd/ack", this::onAck);
        // 只有遗嘱产生的 offline 状态会驱动核心断开；online 心跳不改变状态
        broker.subscribe("gateway", "devices/+/status", (t, p) -> {
            if ("offline".equals(new String(p, StandardCharsets.UTF_8))) {
                String deviceId = extractDeviceId(t, 1);
                core.disconnect(deviceId, ProtocolType.MQTT);
                log.info("[MQTT] 收到遗嘱 offline，设备离线 device={}", deviceId);
            }
        });
    }

    public EmbeddedMqttBroker broker() {
        return broker;
    }

    /** 设备建立 MQTT 连接：完成 Broker 层鉴权与订阅（此阶段还不下发命令）。 */
    public String deviceAttach(String deviceId, String secret,
                               java.util.function.BiConsumer<String, byte[]> commandListener) {
        String clientId = "device-mqtt-" + deviceId;
        String willTopic = "devices/" + deviceId + "/status";
        broker.connect(clientId, deviceId, secret,
                new EmbeddedMqttBroker.WillMessage(willTopic, "offline".getBytes(StandardCharsets.UTF_8), true));
        // 先订阅命令主题
        broker.subscribe(clientId, "devices/" + deviceId + "/commands/#", commandListener);
        return clientId;
    }

    /** 鉴权与订阅完成后建立核心会话，并发布 retained online；此刻才触发离线命令补齐。 */
    public void deviceOnline(String deviceId, String secret, String clientId, DeviceSession session) {
        String willTopic = "devices/" + deviceId + "/status";
        core.connect(deviceId, secret, session);
        broker.publish(clientId, willTopic, "online".getBytes(StandardCharsets.UTF_8), true);
        log.info("[MQTT] 设备上线 device={}", deviceId);
    }

    public void deviceDisconnect(String clientId, String deviceId, boolean graceful) {
        broker.disconnect(clientId, !graceful);
        if (graceful) {
            core.disconnect(deviceId, ProtocolType.MQTT);
        }
        log.info("[MQTT] 设备{}断连 device={}", graceful ? "主动" : "异常(遗嘱)", deviceId);
    }

    private void authenticateConnect(String clientId, String username, String password) {
        try {
            core.authenticate(username, password);
        } catch (GatewayException ex) {
            throw new MqttRejectException(ex.reason().code(), ex.getMessage());
        }
    }

    private void onTelemetry(String topic, byte[] payload) {
        String deviceId = extractDeviceId(topic, 1);
        JsonNode json;
        try {
            json = JsonSupport.mapper().readTree(payload);
        } catch (Exception parseError) {
            log.warn("[MQTT] 拒绝遥测 device={} reason=MALFORMED_PAYLOAD", deviceId);
            throw new MqttRejectException(RejectReason.MALFORMED_PAYLOAD.code(), "payload 不是合法 JSON");
        }
        TelemetryIngest ingest = parseTelemetry(deviceId, json);
        try {
            var result = core.ingest(deviceId, json.path("secret").asText(null), ingest);
            if (!result.accepted()) {
                log.info("[MQTT] 遥测被旧数据保护忽略 device={} reason={}", deviceId, result.reason());
            }
        } catch (GatewayException ex) {
            log.warn("[MQTT] 拒绝遥测 device={} reason={}", deviceId, ex.reason().code());
            throw new MqttRejectException(ex.reason().code(), ex.getMessage());
        }
    }

    private TelemetryIngest parseTelemetry(String topicDeviceId, JsonNode json) {
        JsonNode idNode = json.path("deviceId");
        String deviceId = idNode.isMissingNode() || idNode.isNull() ? topicDeviceId : idNode.asText();
        if (deviceId == null || deviceId.isBlank() || !deviceId.equals(topicDeviceId)) {
            throw new MqttRejectException(RejectReason.MALFORMED_PAYLOAD.code(),
                    "deviceId 缺失或与主题不一致");
        }
        JsonNode props = json.get("properties");
        if (props != null && !props.isObject()) {
            throw new MqttRejectException(RejectReason.MALFORMED_PAYLOAD.code(), "properties 必须是对象");
        }
        Long ts = json.hasNonNull("ts") ? json.get("ts").asLong() : null;
        Long seq = json.hasNonNull("seq") ? json.get("seq").asLong() : null;
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = props == null || props.isNull()
                ? new HashMap<>() : JsonSupport.mapper().convertValue(props, Map.class);
        return new TelemetryIngest(deviceId, ts, seq, properties, ProtocolType.MQTT);
    }

    private void onAck(String topic, byte[] payload) {
        String deviceId = extractDeviceId(topic, 1);
        JsonNode json;
        try {
            json = JsonSupport.mapper().readTree(payload);
        } catch (Exception e) {
            throw new MqttRejectException(RejectReason.MALFORMED_PAYLOAD.code(), "ack 不是合法 JSON");
        }
        String commandId = json.path("commandId").asText(null);
        String secret = json.path("secret").asText(null);
        if (commandId == null || commandId.isBlank()) {
            throw new MqttRejectException(RejectReason.MALFORMED_PAYLOAD.code(), "commandId 必填");
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> result = json.has("result") && json.get("result").isObject()
                    ? JsonSupport.mapper().convertValue(json.get("result"), Map.class) : new HashMap<>();
            @SuppressWarnings("unchecked")
            Map<String, Object> reported = json.has("reported") && json.get("reported").isObject()
                    ? JsonSupport.mapper().convertValue(json.get("reported"), Map.class) : null;
            core.ackCommand(deviceId, secret, commandId, result, reported);
            log.info("[MQTT] 命令确认 device={} commandId={}", deviceId, commandId);
        } catch (GatewayException ex) {
            throw new MqttRejectException(ex.reason().code(), ex.getMessage());
        }
    }

    /** 网关通过 MQTT 向设备发布命令。 */
    public void publishCommand(Command command) {
        String topic = "devices/" + command.deviceId() + "/commands/" + command.commandId();
        Map<String, Object> body = new HashMap<>();
        body.put("commandId", command.commandId());
        body.put("name", command.name());
        body.put("params", command.params());
        byte[] payload;
        try {
            payload = JsonSupport.mapper().writeValueAsBytes(body);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        broker.publish("gateway", topic, payload, false);
    }

    static String extractDeviceId(String topic, int index) {
        String[] parts = topic.split("/");
        return parts.length > index ? parts[index] : null;
    }
}
