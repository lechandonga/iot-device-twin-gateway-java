package com.github.lechandonga.iotdevicetwingatewayjava.model;

import java.util.Map;

/**
 * 设备影子：reported 为设备实际上报值，desired 为云端期望值。
 * 当 desired 中的每个键都出现在 reported 且值相等时，syncState=IN_SYNC，否则 OUT_OF_SYNC。
 */
public record DeviceShadow(
        String deviceId,
        boolean online,
        long lastUpdatedTs,
        Map<String, Object> reported,
        Map<String, Object> desired,
        String syncState,
        long version
) {
}
