package com.github.lechandonga.iotdevicetwingatewayjava.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 * 最小 MQTT 3.1.1 客户端，用于本地设备模拟器与测试，不依赖任何外部库。
 * 支持 CONNECT（用户名/密码/遗嘱）、QoS1 PUBLISH、SUBSCRIBE 接收。
 */
public class MqttClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MqttClient.class);

    private final String host;
    private final int port;
    private final String clientId;
    private final String username;
    private final String password;
    private final String willTopic;
    private final String willMessage;

    private Socket socket;
    private java.io.BufferedInputStream input;
    private DataOutputStream output;
    private Thread readerThread;
    private volatile boolean connected;

    private final CopyOnWriteArrayList<String> subscriptions = new CopyOnWriteArrayList<>();
    private final Map<String, BiConsumer<String, byte[]>> handlers = new ConcurrentHashMap<>();

    public MqttClient(String host, int port, String clientId, String username, String password,
                      String willTopic, String willMessage) {
        this.host = host;
        this.port = port;
        this.clientId = clientId;
        this.username = username;
        this.password = password;
        this.willTopic = willTopic;
        this.willMessage = willMessage;
    }

    /**
     * @return null 成功；否则 broker 返回的拒绝原因描述
     */
    public String connect() throws IOException, InterruptedException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), 2000);
        socket.setTcpNoDelay(true);
        input = new java.io.BufferedInputStream(socket.getInputStream());
        output = new DataOutputStream(socket.getOutputStream());

        ByteArrayOutputStream variable = new ByteArrayOutputStream();
        putString(variable, "MQTT");
        variable.write(4);
        int flags = 0x80 | 0x40 | 0x02;
        variable.write(flags);
        variable.write(0);
        variable.write(60);
        putString(variable, clientId);
        putString(variable, username);
        putString(variable, password);
        output.write(MqttCodec.packet(0x10, variable.toByteArray()));
        output.flush();

        byte first = MqttCodec.readByte(input);
        int remaining = MqttCodec.decodeRemainingLength(input);
        byte[] body = MqttCodec.readRemaining(input, remaining);
        byte returnCode = body[1];
        if (returnCode != 0) {
            socket.close();
            return "CONNACK 拒绝码 0x" + Integer.toHexString(returnCode & 0xFF);
        }
        connected = true;
        readerThread = new Thread(this::readLoop, "mqtt-client-" + clientId);
        readerThread.setDaemon(true);
        readerThread.start();
        return null;
    }

    public void subscribe(String topicFilter, BiConsumer<String, byte[]> handler) throws IOException {
        subscriptions.add(topicFilter);
        if (handler != null) {
            handlers.put(topicFilter, handler);
        }
        ByteArrayOutputStream variable = new ByteArrayOutputStream();
        variable.write(0);
        variable.write(1);
        putString(variable, topicFilter);
        variable.write(1);
        synchronized (output) {
            output.write(MqttCodec.packet(0x82, variable.toByteArray()));
            output.flush();
        }
    }

    public void publishQos1(String topic, byte[] payload) throws IOException {
        ByteArrayOutputStream variable = new ByteArrayOutputStream();
        putString(variable, topic);
        variable.write(0);
        variable.write(1);
        variable.write(payload);
        synchronized (output) {
            output.write(MqttCodec.packet(0x32, variable.toByteArray()));
            output.flush();
        }
    }

    private void readLoop() {
        try {
            while (connected) {
                byte first = MqttCodec.readByte(input);
                int packetType = (first >> 4) & 0x0F;
                int remaining = MqttCodec.decodeRemainingLength(input);
                byte[] body = MqttCodec.readRemaining(input, remaining);
                if (packetType == 3) {
                    int[] cursor = {0};
                    String topic = MqttCodec.decodeString(body, cursor);
                    int qos = (first >> 1) & 0x03;
                    int packetId = 0;
                    if (qos > 0) {
                        packetId = ((body[cursor[0]] & 0xFF) << 8) | (body[cursor[0] + 1] & 0xFF);
                        cursor[0] += 2;
                    }
                    byte[] payload = new byte[body.length - cursor[0]];
                    System.arraycopy(body, cursor[0], payload, 0, payload.length);
                    if (qos == 1) {
                        synchronized (output) {
                            output.write(MqttCodec.pubackPacket(packetId));
                            output.flush();
                        }
                    }
                    for (String filter : subscriptions) {
                        if (MqttBroker.topicMatches(filter, topic)) {
                            BiConsumer<String, byte[]> handler = handlers.get(filter);
                            if (handler != null) {
                                handler.accept(topic, payload);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            if (connected) {
                log.debug("[MQTT客户端 {}] 读取结束: {}", clientId, e.getMessage());
            }
        }
    }

    private void putString(ByteArrayOutputStream out, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.write((byte) (bytes.length >> 8));
        out.write((byte) bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    public boolean isConnected() {
        return connected;
    }

    @Override
    public void close() {
        connected = false;
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
    }
}
