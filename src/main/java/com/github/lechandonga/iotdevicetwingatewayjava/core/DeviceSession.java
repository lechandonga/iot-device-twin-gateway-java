package com.github.lechandonga.iotdevicetwingatewayjava.core;

import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;

/**
 * 设备在线会话（MQTT 连接 或 HTTP 长轮询会话）。命令通过会话投递，
 * 与命令在核心层的状态机完全解耦：投递失败不会改变 QUEUED 语义。
 */
public interface DeviceSession {

    ProtocolType protocol();

    /**
     * 向设备投递一条命令。
     * @throws TransportException 传输层暂时不可用，核心层将保留队列稍后重试
     */
    void deliver(Command command) throws TransportException;

    final class TransportException extends Exception {
        public TransportException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
