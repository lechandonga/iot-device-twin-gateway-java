package com.github.lechandonga.iotdevicetwingatewayjava.model;

/**
 * 稳定可区分的拒绝原因。HTTP 响应体会原样返回 {@link #code()}，
 * MQTT 应答报文的 reason 字段也使用同一套取值。
 */
public enum RejectReason {
    DEVICE_NOT_REGISTERED("设备未注册"),
    BAD_CREDENTIALS("设备凭据错误"),
    MALFORMED_PAYLOAD("报文畸形：无法解析或缺少必填字段"),
    DEVICE_OVERLOADED("设备处理队列已满，拒绝过载请求"),
    GATEWAY_OVERLOADED("网关处理能力不足，拒绝过载请求"),
    UNAUTHORIZED("北向接口管理凭据缺失或错误"),
    UNKNOWN_COMMAND("命令不存在或不属于该设备"),
    CONFLICT("设备会话冲突");

    private final String description;

    RejectReason(String description) {
        this.description = description;
    }

    public String code() {
        return name();
    }

    public String description() {
        return description;
    }
}
