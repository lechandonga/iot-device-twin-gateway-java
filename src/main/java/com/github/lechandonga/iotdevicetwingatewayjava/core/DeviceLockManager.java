package com.github.lechandonga.iotdevicetwingatewayjava.core;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 按设备分片锁：同一设备的并发上报/下发串行化，不同设备互不阻塞。
 */
@Component
public class DeviceLockManager {

    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public ReentrantLock lockFor(String deviceId) {
        return locks.computeIfAbsent(deviceId, key -> new ReentrantLock());
    }
}
