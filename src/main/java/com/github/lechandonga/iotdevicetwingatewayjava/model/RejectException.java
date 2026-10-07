package com.github.lechandonga.iotdevicetwingatewayjava.model;

/**
 * 携带稳定原因码的业务拒绝异常，HTTP / MQTT 两种接入共用。
 */
public class RejectException extends RuntimeException {

    private final ErrorCode errorCode;

    public RejectException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
