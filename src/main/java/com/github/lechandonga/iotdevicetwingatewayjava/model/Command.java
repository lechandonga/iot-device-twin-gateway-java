package com.github.lechandonga.iotdevicetwingatewayjava.model;

import java.util.Map;

public record Command(
        String commandId,
        String deviceId,
        String name,
        Map<String, Object> payload,
        long createdAt,
        long deliveredAt,
        long ackedAt,
        CommandStatus status,
        int attempts,
        String transport,
        String result
) {
    public Command withStatus(CommandStatus newStatus, long deliveredAt, long ackedAt, int attempts, String result) {
        return new Command(commandId, deviceId, name, payload, createdAt, deliveredAt, ackedAt,
                newStatus, attempts, transport, result);
    }
}
