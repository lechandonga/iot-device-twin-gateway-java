package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayCore;
import org.springframework.stereotype.Component;

/** 让长轮询控制器可延迟拿到核心 Bean（保持构造简洁）。 */
@Component
public class GatewayCoreProvider {

    private final GatewayCore core;

    public GatewayCoreProvider(GatewayCore core) {
        this.core = core;
    }

    public GatewayCore core() {
        return core;
    }
}
