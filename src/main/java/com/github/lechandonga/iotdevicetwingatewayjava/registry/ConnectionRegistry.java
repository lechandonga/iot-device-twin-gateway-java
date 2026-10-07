package com.github.lechandonga.iotdevicetwingatewayjava.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备连接状态。MQTT 通过 CONNECT/LWT 驱动；HTTP 通过 connect/disconnect/heartbeat 驱动。
 */
@Component
public class ConnectionRegistry {

    private static final Logger log = LoggerFactory.getLogger(ConnectionRegistry.class);

    private final Set<String> onlineDevices = ConcurrentHashMap.newKeySet();
    private final Set<String> connectedEver = ConcurrentHashMap.newKeySet();

    public void online(String deviceId, String transport) {
        if (onlineDevices.add(deviceId)) {
            connectedEver.add(deviceId);
            log.info("[连接] 设备 {} 通过 {} 上线", deviceId, transport);
        }
    }

    public void offline(String deviceId, String transport, String reason) {
        if (onlineDevices.remove(deviceId)) {
            log.info("[连接] 设备 {} 通过 {} 离线（{}）", deviceId, transport, reason);
        }
    }

    public boolean isOnline(String deviceId) {
        return onlineDevices.contains(deviceId);
    }

    public Set<String> onlineDevices() {
        return Set.copyOf(onlineDevices);
    }

    public void clear() {
        onlineDevices.clear();
        connectedEver.clear();
    }
}
