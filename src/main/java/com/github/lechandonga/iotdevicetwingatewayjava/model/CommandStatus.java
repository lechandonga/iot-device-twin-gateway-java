package com.github.lechandonga.iotdevicetwingatewayjava.model;

/**
 * 命令生命周期：
 * QUEUED（设备离线，已持久化入队）
 *   -> IN_FLIGHT（已下发，等待 ack）
 *   -> ACKED（设备确认，终态成功）
 *   -> TIMEOUT（超时未确认，可重试）
 *   -> IN_FLIGHT（重试下发）
 * 重试次数用尽后为 FAILED（终态失败）。
 */
public enum CommandStatus {
    QUEUED, IN_FLIGHT, ACKED, TIMEOUT, FAILED
}
