package com.github.lechandonga.iotdevicetwingatewayjava.core;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /** 全局可并发处理的请求数（超出立即拒绝）。 */
    private int capacity = 64;

    /** 单个设备待确认/待投递命令队列上限。 */
    private int commandQueueCapacity = 100;

    /** 命令投递后等待 ACK 的毫秒数，超时可重试。 */
    private long commandAckTimeoutMs = 2_000;

    /** 单条遥测处理占用容量闸门的毫秒数（默认 0；测试可调大以稳定制造过载）。 */
    private long processingDelayMs = 0;

    /** 单条命令最大投递次数，超过则 EXPIRED。 */
    private int commandMaxAttempts = 3;

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getCommandQueueCapacity() {
        return commandQueueCapacity;
    }

    public void setCommandQueueCapacity(int commandQueueCapacity) {
        this.commandQueueCapacity = commandQueueCapacity;
    }

    public long getCommandAckTimeoutMs() {
        return commandAckTimeoutMs;
    }

    public void setCommandAckTimeoutMs(long commandAckTimeoutMs) {
        this.commandAckTimeoutMs = commandAckTimeoutMs;
    }

    public long getProcessingDelayMs() {
        return processingDelayMs;
    }

    public void setProcessingDelayMs(long processingDelayMs) {
        this.processingDelayMs = processingDelayMs;
    }

    public int getCommandMaxAttempts() {
        return commandMaxAttempts;
    }

    public void setCommandMaxAttempts(int commandMaxAttempts) {
        this.commandMaxAttempts = commandMaxAttempts;
    }
}
