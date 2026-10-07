package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.core.CommandService;
import com.github.lechandonga.iotdevicetwingatewayjava.core.ShadowService;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceShadow;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ErrorCode;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectException;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.ConnectionRegistry;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.DeviceRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 云端（业务侧）API：设备注册、影子期望、命令下发与结果查询。
 */
@RestController
@RequestMapping("/api/v1/cloud")
public class CloudController {

    private final DeviceRegistry deviceRegistry;
    private final ConnectionRegistry connectionRegistry;
    private final ShadowService shadowService;
    private final CommandService commandService;

    public CloudController(DeviceRegistry deviceRegistry, ConnectionRegistry connectionRegistry,
                           ShadowService shadowService, CommandService commandService) {
        this.deviceRegistry = deviceRegistry;
        this.connectionRegistry = connectionRegistry;
        this.shadowService = shadowService;
        this.commandService = commandService;
    }

    @PostMapping("/devices")
    public Map<String, Object> register(@RequestBody Map<String, Object> body) {
        String deviceId = String.valueOf(body.get("deviceId"));
        String secret = String.valueOf(body.get("secret"));
        if (deviceId == null || deviceId.isBlank() || "null".equals(deviceId)
                || secret == null || secret.isBlank() || "null".equals(secret)) {
            throw new RejectException(ErrorCode.MALFORMED_PAYLOAD, "deviceId 与 secret 必填");
        }
        deviceRegistry.register(deviceId, secret);
        return Map.of("deviceId", deviceId, "registered", true);
    }

    @GetMapping("/devices")
    public Map<String, Object> devices() {
        return Map.of("online", connectionRegistry.onlineDevices());
    }

    @GetMapping("/devices/{deviceId}/shadow")
    public DeviceShadow shadow(@PathVariable String deviceId) {
        if (!deviceRegistry.exists(deviceId)) {
            throw new RejectException(ErrorCode.DEVICE_NOT_REGISTERED, "设备 " + deviceId + " 未注册");
        }
        return shadowService.getShadow(deviceId);
    }

    @PostMapping("/devices/{deviceId}/desired")
    public DeviceShadow desired(@PathVariable String deviceId, @RequestBody Map<String, Object> patch) {
        if (!deviceRegistry.exists(deviceId)) {
            throw new RejectException(ErrorCode.DEVICE_NOT_REGISTERED, "设备 " + deviceId + " 未注册");
        }
        return shadowService.setDesired(deviceId, patch);
    }

    @PostMapping("/devices/{deviceId}/commands")
    public Command issue(@PathVariable String deviceId,
                         @RequestParam(value = "transport", defaultValue = "MQTT") String transport,
                         @RequestBody Map<String, Object> body) {
        if (!deviceRegistry.exists(deviceId)) {
            throw new RejectException(ErrorCode.DEVICE_NOT_REGISTERED, "设备 " + deviceId + " 未注册");
        }
        String name = String.valueOf(body.getOrDefault("name", "defaultCommand"));
        Object payloadObject = body.get("payload");
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = payloadObject instanceof Map<?, ?> map
                ? new LinkedHashMap<>((Map<String, Object>) map) : Map.of();
        return commandService.issue(deviceId, name, payload, transport.toUpperCase());
    }

    @GetMapping("/commands/{commandId}")
    public Command command(@PathVariable String commandId) {
        return commandService.getCommand(commandId);
    }

    @GetMapping("/devices/{deviceId}/commands")
    public List<Command> commands(@PathVariable String deviceId) {
        return commandService.listCommands(deviceId);
    }

    @GetMapping("/commands/events")
    public List<String> events() {
        return commandService.eventLog();
    }
}
