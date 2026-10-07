package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.core.CommandPublisher;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HTTP 设备通过长轮询拉取命令：无等待中的拉取请求时投递失败，命令保持排队。
 */
@Component
public class HttpCommandPublisher implements CommandPublisher {

    private final Map<String, CompletableFuture<Command>> waitingPolls = new ConcurrentHashMap<>();

    public void registerPoll(String deviceId, CompletableFuture<Command> future) {
        waitingPolls.put(deviceId, future);
        future.whenComplete((command, error) -> waitingPolls.remove(deviceId, future));
    }

    @Override
    public String transport() {
        return "HTTP";
    }

    @Override
    public boolean publish(Command command) {
        CompletableFuture<Command> future = waitingPolls.get(command.deviceId());
        if (future == null) {
            return false;
        }
        return future.complete(command);
    }
}
