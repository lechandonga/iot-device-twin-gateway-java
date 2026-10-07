package com.github.lechandonga.iotdevicetwingatewayjava.model;

/**
 * 业务拒绝异常，携带稳定的 {@link RejectReason}，由两种接入协议的边界层统一翻译成协议应答。
 */
public class GatewayException extends RuntimeException {
    private final RejectReason reason;

    public GatewayException(RejectReason reason) {
        super(reason.code() + ": " + reason.description());
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
