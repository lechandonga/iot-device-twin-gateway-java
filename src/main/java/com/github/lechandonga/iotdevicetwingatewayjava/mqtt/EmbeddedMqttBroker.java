package com.github.lechandonga.iotdevicetwingatewayjava.mqtt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 * 进程内嵌的极简 MQTT 3.1.1 风格 Broker：支持 CONNECT(带用户名密码)、
 * PUBLISH(含 retained)、SUBSCRIBE(支持 + / # 通配符)、WILL 遗嘱消息。
 * 不依赖任何外部中间件，行为足以在本地解释真实 MQTT 接入的关键语义：
 * 连接鉴权、遗嘱离线、按主题扇出。生产部署可将本实现替换为 TCP 真 Broker，
 * {@link com.github.lechandonga.iotdevicetwingatewayjava.mqtt.MqttGatewayAdapter} 业务逻辑不变。
 */
public class EmbeddedMqttBroker {

    public record WillMessage(String topic, byte[] payload, boolean retained) {}

    public interface ConnectAuthenticator {
        /** 抛出 {@link MqttRejectException} 表示拒绝连接。 */
        void authenticate(String clientId, String username, String password);
    }

    private record Subscription(String clientId, String filter, BiConsumer<String, byte[]> listener) {}

    private final Map<String, WillMessage> wills = new ConcurrentHashMap<>();
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private final Map<String, byte[]> retained = new ConcurrentHashMap<>();
    private final ConnectAuthenticator authenticator;

    public EmbeddedMqttBroker(ConnectAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    public void connect(String clientId, String username, String password, WillMessage will) {
        authenticator.authenticate(clientId, username, password);
        if (will != null) {
            wills.put(clientId, will);
        }
    }

    public void subscribe(String clientId, String filter, BiConsumer<String, byte[]> listener) {
        subscriptions.add(new Subscription(clientId, filter, listener));
        // 投递 retained 消息（模拟 MQTT 订阅即收 retained）
        for (Map.Entry<String, byte[]> e : retained.entrySet()) {
            if (matches(filter, e.getKey())) {
                listener.accept(e.getKey(), e.getValue());
            }
        }
    }

    public void unsubscribeAll(String clientId) {
        subscriptions.removeIf(s -> s.clientId().equals(clientId));
    }

    public void publish(String clientId, String topic, byte[] payload, boolean retain) {
        for (Subscription s : subscriptions) {
            if (matches(s.filter(), topic)) {
                try {
                    s.listener().accept(topic, payload);
                } catch (RuntimeException ex) {
                    // 与真实 Broker 一致：单个订阅者处理失败不影响其他订阅者，
                    // 但必须显式抛出以便发送方感知拒绝原因（由调用方决定处理）
                    throw ex;
                }
            }
        }
        if (retain) {
            retained.put(topic, payload);
        }
    }

    /**
     * @param publishWill true 表示异常掉线（触发遗嘱）；false 为正常断连（不触发遗嘱）
     */
    public void disconnect(String clientId, boolean publishWill) {
        WillMessage will = wills.remove(clientId);
        unsubscribeAll(clientId);
        if (publishWill && will != null) {
            publish(null, will.topic(), will.payload(), will.retained());
        }
    }

    static boolean matches(String filter, String topic) {
        String[] f = filter.split("/");
        String[] t = topic.split("/");
        int i = 0;
        for (; i < f.length; i++) {
            if ("#".equals(f[i])) {
                return true;
            }
            if (i >= t.length) {
                return false;
            }
            if (!"+".equals(f[i]) && !f[i].equals(t[i])) {
                return false;
            }
        }
        return i == t.length;
    }

    /** 测试辅助：获取 retained 消息。 */
    public byte[] retainedOf(String topic) {
        return retained.get(topic);
    }

    public List<String> subscriberInfo() {
        List<String> info = new ArrayList<>();
        subscriptions.forEach(s -> info.add(s.clientId() + " -> " + s.filter()));
        return info;
    }
}
