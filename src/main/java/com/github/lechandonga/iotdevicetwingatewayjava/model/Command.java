package com.github.lechandonga.iotdevicetwingatewayjava.model;

import java.time.Instant;
import java.util.Map;

/** 下发给设备的命令。离线期间进入 QUEUED，重连后按 createdAt 顺序补齐。 */
public record Command(String commandId,
                      String deviceId,
                      String name,
                      Map<String, Object> params,
                      CommandStatus status,
                      int attempts,
                      int maxAttempts,
                      Instant createdAt,
                      Instant lastDispatchedAt,
                      Instant ackedAt,
                      Map<String, Object> ackResult,
                      String lastError) {

    public Command withStatus(CommandStatus newStatus, Instant dispatchedAt, String error) {
        return new Command(commandId, deviceId, name, params, newStatus,
                newStatus == CommandStatus.IN_FLIGHT ? attempts + 1 : attempts,
                maxAttempts, createdAt, dispatchedAt, ackedAt, ackResult, error);
    }

    public Command acked(Map<String, Object> result, Instant at) {
        return new Command(commandId, deviceId, name, params, CommandStatus.ACKED,
                attempts, maxAttempts, createdAt, lastDispatchedAt, at, result, null);
    }
}
