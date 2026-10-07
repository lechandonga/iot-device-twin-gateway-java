package com.github.lechandonga.iotdevicetwingatewayjava.core;

import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;

/**
 * 传输层投递实现：MQTT 发布到 cmd 主题；HTTP 由设备长轮询拉取。
 */
public interface CommandPublisher {

    String transport();

    /**
     * 尝试向在线设备投递一条命令。
     *
     * @return true 已送达传输层（开始等待 ACK）；false 当前无法送达（如 HTTP 设备没有等待中的拉取请求）
     */
    boolean publish(Command command);
}
