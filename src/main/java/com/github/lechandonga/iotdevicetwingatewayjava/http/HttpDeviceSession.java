package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.core.DeviceSession;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HTTP 接入的设备会话：以长轮询模拟服务端推送。
 * 设备侧 GET /commands 时从队列取命令；命令投递与 MQTT 的 PUBLISH 语义等价，
 * 离线时核心层保留 QUEUED，重连（再次轮询）后按原顺序补齐。
 */
public class HttpDeviceSession implements DeviceSession {

    private final String deviceId;
    private final LinkedBlockingQueue<Command> inbox = new LinkedBlockingQueue<>(256);
    private final AtomicBoolean polling = new AtomicBoolean(false);
    private volatile boolean active = true;

    public HttpDeviceSession(String deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public ProtocolType protocol() {
        return ProtocolType.HTTP;
    }

    @Override
    public void deliver(Command command) throws TransportException {
        if (!active) {
            throw new TransportException("session inactive", null);
        }
        if (!inbox.offer(command)) {
            throw new TransportException("inbox full", null);
        }
    }

    public void markPolling() {
        polling.set(true);
    }

    public Command poll(long timeoutMillis) throws InterruptedException {
        polling.set(true);
        try {
            return inbox.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        } finally {
            // 轮询间隙视为仍在线（模拟器会立刻发起下一次轮询），显式 DELETE 才标记离线
        }
    }

    public void deactivate() {
        active = false;
        polling.set(false);
    }

    public boolean isActive() {
        return active;
    }

    public String deviceId() {
        return deviceId;
    }
}
