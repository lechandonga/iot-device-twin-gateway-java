package com.github.lechandonga.iotdevicetwingatewayjava.mqtt;

import com.github.lechandonga.iotdevicetwingatewayjava.core.CommandPublisher;
import com.github.lechandonga.iotdevicetwingatewayjava.core.CommandService;
import com.github.lechandonga.iotdevicetwingatewayjava.core.TelemetryService;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.ConnectionRegistry;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.DeviceRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 内嵌最小 MQTT 3.1.1 Broker，纯 JDK 实现，无需外部中间件。
 * 支持：CONNECT 鉴权、QoS1 PUBLISH/PUBACK、SUBSCRIBE（含 +/# 通配符）、PINGREQ、遗嘱消息。
 */
@Component
public class MqttBroker {

    private static final Logger log = LoggerFactory.getLogger(MqttBroker.class);

    private final int port;
    private final DeviceRegistry deviceRegistry;
    private final ConnectionRegistry connectionRegistry;
    private final TelemetryService telemetryService;
    private final CommandService commandService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private ServerSocket serverSocket;
    private Thread acceptThread;
    private volatile boolean running;

    /** topicFilter -> 订阅连接列表 */
    private final Map<String, List<MqttSession>> subscribers = new ConcurrentHashMap<>();
    private final List<MqttSession> sessions = new CopyOnWriteArrayList<>();
    private final AtomicInteger packetIdSequence = new AtomicInteger();

    public MqttBroker(@Value("${gateway.mqtt.port:1883}") int port,
                      DeviceRegistry deviceRegistry,
                      ConnectionRegistry connectionRegistry,
                      TelemetryService telemetryService,
                      CommandService commandService) {
        this.port = port;
        this.deviceRegistry = deviceRegistry;
        this.connectionRegistry = connectionRegistry;
        this.telemetryService = telemetryService;
        this.commandService = commandService;
    }

    @PostConstruct
    public void start() {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new IllegalStateException("MQTT broker 监听端口 " + port + " 失败", e);
        }
        running = true;
        acceptThread = new Thread(this::acceptLoop, "mqtt-broker-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        commandService.registerPublisher(new MqttCommandPublisher());
        log.info("[MQTT] 内嵌 Broker 已启动，端口 {}", serverSocket.getLocalPort());
    }

    @PreDestroy
    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
        sessions.forEach(MqttSession::close);
    }

    public int getBoundPort() {
        return serverSocket == null ? port : serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                MqttSession session = new MqttSession(socket);
                sessions.add(session);
                Thread thread = new Thread(() -> serve(session), "mqtt-session");
                thread.setDaemon(true);
                thread.start();
            } catch (IOException e) {
                if (running) {
                    log.warn("[MQTT] accept 失败: {}", e.getMessage());
                }
            }
        }
    }

    private void serve(MqttSession session) {
        try {
            handleConnect(session);
            while (running && session.open) {
                byte first = MqttCodec.readByte(session.input);
                int packetType = (first >> 4) & 0x0F;
                int remainingLength = MqttCodec.decodeRemainingLength(session.input);
                byte[] body = MqttCodec.readRemaining(session.input, remainingLength);
                switch (packetType) {
                    case 3 -> handlePublish(session, first, body);
                    case 4 -> {
                    }
                    case 8 -> handleSubscribe(session, body);
                    case 12 -> session.write(MqttCodec.PINGRESP);
                    case 14 -> {
                        session.open = false;
                    }
                    default -> {
                    }
                }
            }
        } catch (EOFException e) {
            // 对端关闭
        } catch (Exception e) {
            if (session.open) {
                log.debug("[MQTT] 会话结束: {}", e.getMessage());
            }
        } finally {
            sessionTerminated(session);
        }
    }

    private void handleConnect(MqttSession session) throws IOException {
        byte first = MqttCodec.readByte(session.input);
        if (((first >> 4) & 0x0F) != 1) {
            throw new IOException("首报文必须是 CONNECT");
        }
        int remainingLength = MqttCodec.decodeRemainingLength(session.input);
        byte[] body = MqttCodec.readRemaining(session.input, remainingLength);
        int[] cursor = {0};
        MqttCodec.decodeString(body, cursor);
        int level = body[cursor[0]++] & 0xFF;
        int flags = body[cursor[0]++] & 0xFF;
        cursor[0] += 2;

        String clientId = MqttCodec.decodeString(body, cursor);
        String username = (flags & 0x80) != 0 ? MqttCodec.decodeString(body, cursor) : null;
        String password = (flags & 0x40) != 0 ? MqttCodec.decodeString(body, cursor) : null;

        String willTopic = null;
        byte[] willMessage = null;
        if ((flags & 0x04) != 0) {
            willTopic = MqttCodec.decodeString(body, cursor);
            int willLength = ((body[cursor[0]] & 0xFF) << 8) | (body[cursor[0] + 1] & 0xFF);
            cursor[0] += 2;
            willMessage = new byte[willLength];
            System.arraycopy(body, cursor[0], willMessage, 0, willLength);
            cursor[0] += willLength;
        }

        String deviceId = username != null ? username : clientId;
        String errorCode = deviceRegistry.checkCredentials(deviceId, password);
        if (errorCode != null) {
            byte rc = switch (errorCode) {
                case "DEVICE_NOT_REGISTERED" -> 0x05;
                case "MISSING_CREDENTIALS" -> 0x04;
                default -> 0x04;
            };
            session.write(MqttCodec.connackPacket(false, rc));
            log.warn("[MQTT] CONNECT 被拒绝 deviceId={} 原因={}", deviceId, errorCode);
            session.open = false;
            session.socket.close();
            return;
        }

        session.deviceId = deviceId;
        session.willTopic = willTopic;
        session.willMessage = willMessage;
        session.write(MqttCodec.connackPacket(false, (byte) 0x00));
        connectionRegistry.online(deviceId, "MQTT");
        commandService.onDeviceConnected(deviceId, "MQTT");
        log.info("[MQTT] 设备 {} 连接成功（协议级别 {}）", deviceId, level);
    }

    private void handlePublish(MqttSession session, byte first, byte[] body) throws IOException {
        int[] cursor = {0};
        String topic = MqttCodec.decodeString(body, cursor);
        int qos = (first >> 1) & 0x03;
        Integer packetId = null;
        if (qos > 0) {
            packetId = ((body[cursor[0]] & 0xFF) << 8) | (body[cursor[0] + 1] & 0xFF);
            cursor[0] += 2;
        }
        byte[] payload = new byte[body.length - cursor[0]];
        System.arraycopy(body, cursor[0], payload, 0, payload.length);

        if (packetId != null) {
            session.write(MqttCodec.pubackPacket(packetId));
        }

        if (session.deviceId == null) {
            return;
        }
        try {
            if (topic.startsWith("devices/") && topic.endsWith("/telemetry")) {
                String topicDevice = extractDeviceId(topic);
                if (!session.deviceId.equals(topicDevice)) {
                    log.warn("[MQTT] 设备 {} 尝试替 {} 上报，拒绝", session.deviceId, topicDevice);
                    return;
                }
                telemetryService.ingest(session.deviceId, new String(payload, StandardCharsets.UTF_8), "MQTT");
            } else if (topic.startsWith("devices/") && topic.endsWith("/cmd/ack")) {
                JsonNodeLike ack = parseAck(payload);
                commandService.acknowledge(ack.commandId(), ack.result());
            }
        } catch (Exception e) {
            log.warn("[MQTT] 设备 {} 报文处理失败 topic={} : {}",
                    session.deviceId, topic, e.getMessage());
        }
    }

    private JsonNodeLike parseAck(byte[] payload) throws IOException {
        var node = objectMapper.readTree(payload);
        String commandId = node.path("commandId").asText(null);
        String result = node.path("result").asText("OK");
        return new JsonNodeLike(commandId, result);
    }

    private record JsonNodeLike(String commandId, String result) {
    }

    private void handleSubscribe(MqttSession session, byte[] body) throws IOException {
        int packetId = ((body[0] & 0xFF) << 8) | (body[1] & 0xFF);
        int[] cursor = {2};
        String filter = MqttCodec.decodeString(body, cursor);
        cursor[0]++;
        subscribers.computeIfAbsent(filter, key -> new CopyOnWriteArrayList<>()).add(session);
        session.subscriptions.add(filter);
        session.write(MqttCodec.subackPacket(packetId, (byte) 1));
        log.info("[MQTT] 设备 {} 订阅 {}", session.deviceId, filter);
    }

    private void sessionTerminated(MqttSession session) {
        sessions.remove(session);
        session.subscriptions.forEach(filter -> {
            List<MqttSession> list = subscribers.get(filter);
            if (list != null) {
                list.remove(session);
            }
        });
        if (session.deviceId != null) {
            connectionRegistry.offline(session.deviceId, "MQTT",
                    session.willTopic != null ? "遗嘱消息/连接断开" : "连接断开");
            if (session.willTopic != null && session.willMessage != null) {
                dispatch(session.willTopic, session.willMessage, null);
            }
        }
        session.close();
    }

    void dispatch(String topic, byte[] payload, MqttSession except) {
        List<MqttSession> targets = new ArrayList<>();
        for (Map.Entry<String, List<MqttSession>> entry : subscribers.entrySet()) {
            if (topicMatches(entry.getKey(), topic)) {
                for (MqttSession candidate : entry.getValue()) {
                    if (candidate != except && !targets.contains(candidate)) {
                        targets.add(candidate);
                    }
                }
            }
        }
        for (MqttSession target : targets) {
            int packetId = packetIdSequence.incrementAndGet() & 0xFFFF;
            if (packetId == 0) {
                packetId = 1;
            }
            target.write(MqttCodec.publishPacket(topic, payload, packetId));
        }
    }

    static boolean topicMatches(String filter, String topic) {
        String[] filterParts = filter.split("/");
        String[] topicParts = topic.split("/");
        int index = 0;
        for (; index < filterParts.length; index++) {
            String part = filterParts[index];
            if ("#".equals(part)) {
                return true;
            }
            if (index >= topicParts.length) {
                return false;
            }
            if (!"+".equals(part) && !part.equals(topicParts[index])) {
                return false;
            }
        }
        return index == topicParts.length;
    }

    private static String extractDeviceId(String topic) {
        String[] parts = topic.split("/");
        return parts.length >= 2 ? parts[1] : null;
    }

    private static final class MqttSession {
        final Socket socket;
        final java.io.BufferedInputStream input;
        volatile String deviceId;
        volatile boolean open = true;
        final List<String> subscriptions = new CopyOnWriteArrayList<>();
        String willTopic;
        byte[] willMessage;

        MqttSession(Socket socket) {
            this.socket = socket;
            try {
                this.input = new java.io.BufferedInputStream(socket.getInputStream());
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
            try {
                socket.setTcpNoDelay(true);
            } catch (IOException ignored) {
            }
        }

        synchronized void write(byte[] bytes) {
            try {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.write(bytes);
                out.flush();
            } catch (IOException e) {
                open = false;
            }
        }

        void close() {
            open = false;
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * 通过 MQTT 向设备下发命令。
     */
    private final class MqttCommandPublisher implements CommandPublisher {
        @Override
        public String transport() {
            return "MQTT";
        }

        @Override
        public boolean publish(Command command) {
            MqttSession target = sessions.stream()
                    .filter(session -> command.deviceId().equals(session.deviceId) && session.open)
                    .findFirst()
                    .orElse(null);
            if (target == null) {
                return false;
            }
            try {
                Map<String, Object> message = Map.of(
                        "commandId", command.commandId(),
                        "name", command.name(),
                        "payload", command.payload()
                );
                String topic = "devices/" + command.deviceId() + "/cmd";
                dispatch(topic, objectMapper.writeValueAsBytes(message), null);
                return true;
            } catch (Exception e) {
                log.warn("[MQTT] 命令 {} 投递失败: {}", command.commandId(), e.getMessage());
                return false;
            }
        }
    }
}
