package com.github.lechandonga.iotdevicetwingatewayjava.registry;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备注册表。deviceId -> 注册信息（含密钥）。
 */
@Component
public class DeviceRegistry {

    public record DeviceRecord(String deviceId, String secret) {
    }

    private final Map<String, DeviceRecord> devices = new ConcurrentHashMap<>();

    public void register(String deviceId, String secret) {
        devices.put(deviceId, new DeviceRecord(deviceId, secret));
    }

    public boolean exists(String deviceId) {
        return deviceId != null && devices.containsKey(deviceId);
    }

    /**
     * 校验凭据。
     *
     * @return 校验失败原因；成功返回 null
     */
    public String checkCredentials(String deviceId, String secret) {
        if (deviceId == null || secret == null || secret.isBlank()) {
            return "MISSING_CREDENTIALS";
        }
        DeviceRecord record = devices.get(deviceId);
        if (record == null) {
            return "DEVICE_NOT_REGISTERED";
        }
        if (!record.secret().equals(secret)) {
            return "BAD_CREDENTIALS";
        }
        return null;
    }

    public void clear() {
        devices.clear();
    }
}
