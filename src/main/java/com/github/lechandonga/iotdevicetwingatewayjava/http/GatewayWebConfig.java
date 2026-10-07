package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.core.CommandService;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayWebConfig {

    private final CommandService commandService;
    private final HttpCommandPublisher httpCommandPublisher;

    public GatewayWebConfig(CommandService commandService, HttpCommandPublisher httpCommandPublisher) {
        this.commandService = commandService;
        this.httpCommandPublisher = httpCommandPublisher;
    }

    @PostConstruct
    public void registerHttpPublisher() {
        commandService.registerPublisher(httpCommandPublisher);
    }
}
