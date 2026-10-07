package com.github.lechandonga.iotdevicetwingatewayjava.model;

/**
 * 命令生命周期：
 * QUEUED      已创建，设备离线或待投递
 * DELIVERED   已发送给设备，等待 ACK
 * ACKED       设备已确认
 * TIMED_OUT   投递后超时未确认，可被重试
 * EXPIRED     超过最大投递次数仍未确认
 */
public enum CommandStatus {
    QUEUED,
    DELIVERED,
    ACKED,
    TIMED_OUT,
    EXPIRED
}
