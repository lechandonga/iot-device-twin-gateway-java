package com.github.lechandonga.iotdevicetwingatewayjava.http;

import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** 设备 HTTP 会话（长轮询收件箱）注册表。 */
@Component
public class HttpSessionRegistry {

    private final ConcurrentMap<String, HttpDeviceSession> sessions = new ConcurrentHashMap<>();

    public HttpDeviceSession open(String deviceId) {
        HttpDeviceSession session = new HttpDeviceSession(deviceId);
        sessions.put(deviceId, session);
        return session;
    }

    public Optional<HttpDeviceSession> get(String deviceId) {
        return Optional.ofNullable(sessions.get(deviceId));
    }

    public HttpDeviceSession getOrOpen(String deviceId) {
        return sessions.computeIfAbsent(deviceId, HttpDeviceSession::new);
    }

    public void close(String deviceId) {
        HttpDeviceSession session = sessions.remove(deviceId);
        if (session != null) {
            session.deactivate();
        }
    }
}
