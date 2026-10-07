package com.github.lechandonga.iotdevicetwingatewayjava.model;

import java.util.Map;

/**
 * 归一化后的遥测事件。无论来自 MQTT 还是 HTTP，结构一致。
 */
public record Telemetry(
        String deviceId,
        long ts,
        Map<String, Object> properties,
        String transport
) {
}
