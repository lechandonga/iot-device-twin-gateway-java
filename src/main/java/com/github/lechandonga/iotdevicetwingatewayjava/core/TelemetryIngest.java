package com.github.lechandonga.iotdevicetwingatewayjava.core;

import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;

import java.util.Map;

/**
 * 协议适配层解析后的原始遥测。ts / seq 允许为 null，由核心层按稳定缺省语义归一化，
 * 从而保证 MQTT 与 HTTP 的归一化结论完全一致。
 */
public record TelemetryIngest(String deviceId,
                              Long tsEpochMillis,
                              Long seq,
                              Map<String, Object> properties,
                              ProtocolType protocol) {
}
