package com.github.lechandonga.iotdevicetwingatewayjava.model;

import java.util.Map;

/**
 * 归一化遥测。
 *
 * <p>稳定缺省语义（在协议适配层完成，两种协议行为一致）：
 * <ul>
 *   <li>{@code ts} 缺失：取网关接收时刻；</li>
 *   <li>seq 缺失：按同一设备已见最大序号 +1 派生（保证不破坏旧数据保护）；</li>
 *   <li>properties 缺失或为 null：空 Map；</li>
 *   <li>deviceId 为必填，payload 整体不可解析即 MALFORMED_PAYLOAD。</li>
 * </ul>
 */
public record Telemetry(String deviceId,
                        long tsEpochMillis,
                        long seq,
                        Map<String, Object> properties,
                        ProtocolType protocol,
                        boolean seqDerived) {
}
