package com.github.lechandonga.iotdevicetwingatewayjava.mqtt;

/** MQTT CONNECT/PUBLISH 被拒绝，reason 取值与 RejectReason 对齐。 */
public class MqttRejectException extends RuntimeException {
    private final String reasonCode;

    public MqttRejectException(String reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }

    public String reasonCode() {
        return reasonCode;
    }
}
