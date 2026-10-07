package com.github.lechandonga.iotdevicetwingatewayjava.core;

import com.github.lechandonga.iotdevicetwingatewayjava.model.ErrorCode;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectException;

import java.util.concurrent.Semaphore;

/**
 * 网关整体处理能力闸门：信号量代表可并发处理的上报/下发请求数。
 * 超出能力时立即以 GATEWAY_BUSY 拒绝，不做无界排队。
 */
public class IngestGate {

    private final Semaphore permits;

    public IngestGate(int capacity) {
        this.permits = new Semaphore(capacity);
    }

    public AutoCloseable acquire() {
        if (!permits.tryAcquire()) {
            throw new RejectException(ErrorCode.GATEWAY_BUSY,
                    "网关处理能力已满，请求被明确拒绝（无界堆积保护）");
        }
        return permits::release;
    }
}
