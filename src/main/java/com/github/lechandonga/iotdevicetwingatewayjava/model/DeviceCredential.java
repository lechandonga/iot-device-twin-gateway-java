package com.github.lechandonga.iotdevicetwingatewayjava.model;

import java.time.Instant;

/**
 * 已注册设备的身份记录。
 */
public record DeviceCredential(String deviceId, String secret, Instant registeredAt) {
}
