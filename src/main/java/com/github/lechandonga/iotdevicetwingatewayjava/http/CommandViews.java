package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;

import java.util.LinkedHashMap;
import java.util.Map;

/** 命令的可查询视图：确认状态、尝试次数、超时/重试信息。 */
public final class CommandViews {

    private CommandViews() {
    }

    public static Map<String, Object> of(Command c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("commandId", c.commandId());
        m.put("deviceId", c.deviceId());
        m.put("name", c.name());
        m.put("params", c.params());
        m.put("status", c.status());
        m.put("attempts", c.attempts());
        m.put("maxAttempts", c.maxAttempts());
        m.put("createdAt", c.createdAt() == null ? null : c.createdAt().toEpochMilli());
        m.put("lastDispatchedAt", c.lastDispatchedAt() == null ? null : c.lastDispatchedAt().toEpochMilli());
        m.put("ackedAt", c.ackedAt() == null ? null : c.ackedAt().toEpochMilli());
        m.put("ackResult", c.ackResult());
        m.put("lastError", c.lastError());
        return m;
    }
}
