package com.github.lechandonga.iotdevicetwingatewayjava.model;

/**
 * 稳定可区分的拒绝/处理原因码。
 */
public enum ErrorCode {
    OK,
    DEVICE_NOT_REGISTERED,
    BAD_CREDENTIALS,
    MISSING_CREDENTIALS,
    MALFORMED_PAYLOAD,
    GATEWAY_BUSY,
    COMMAND_QUEUE_FULL,
    COMMAND_NOT_FOUND
}
