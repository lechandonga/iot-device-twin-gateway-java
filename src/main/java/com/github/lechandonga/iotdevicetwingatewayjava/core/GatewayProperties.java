package com.github.lechandonga.iotdevicetwingatewayjava.core;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 网关容量与命令重试参数，均可通过 application.properties 覆盖。 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /** 单设备待处理任务队列容量，超出后返回 DEVICE_OVERLOADED。 */
    private int perDeviceQueueCapacity = 64;

    /** 全局在飞任务上限，超出后返回 GATEWAY_OVERLOADED。 */
    private int globalInflightLimit = 1024;

    /** 命令下发后等待 ack 的毫秒数。 */
    private long commandAckTimeoutMs = 1500;

    /** 命令最大尝试次数（首发 + 重试）。 */
    private int commandMaxAttempts = 3;

    /** 超时扫描间隔。 */
    private long timeoutSweepIntervalMs = 100;

    /** 北向管理接口凭据。 */
    private String adminToken = "admin-token";

    public int getPerDeviceQueueCapacity() { return perDeviceQueueCapacity; }
    public void setPerDeviceQueueCapacity(int v) { this.perDeviceQueueCapacity = v; }
    public int getGlobalInflightLimit() { return globalInflightLimit; }
    public void setGlobalInflightLimit(int v) { this.globalInflightLimit = v; }
    public long getCommandAckTimeoutMs() { return commandAckTimeoutMs; }
    public void setCommandAckTimeoutMs(long v) { this.commandAckTimeoutMs = v; }
    public int getCommandMaxAttempts() { return commandMaxAttempts; }
    public void setCommandMaxAttempts(int v) { this.commandMaxAttempts = v; }
    public long getTimeoutSweepIntervalMs() { return timeoutSweepIntervalMs; }
    public void setTimeoutSweepIntervalMs(long v) { this.timeoutSweepIntervalMs = v; }
    public String getAdminToken() { return adminToken; }
    public void setAdminToken(String v) { this.adminToken = v; }
}
