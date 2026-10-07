package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.core.CommandService;
import com.github.lechandonga.iotdevicetwingatewayjava.core.ShadowService;
import com.github.lechandonga.iotdevicetwingatewayjava.core.TelemetryService;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceShadow;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ErrorCode;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectException;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.ConnectionRegistry;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.DeviceRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 设备侧 HTTP 接入。鉴权头：X-Device-Id + X-Device-Secret。
 */
@RestController
@RequestMapping("/api/v1/devices/{deviceId}")
public class DeviceController {

    private final DeviceRegistry deviceRegistry;
    private final ConnectionRegistry connectionRegistry;
    private final TelemetryService telemetryService;
    private final ShadowService shadowService;
    private final CommandService commandService;
    private final HttpCommandPublisher httpCommandPublisher;

    public DeviceController(DeviceRegistry deviceRegistry, ConnectionRegistry connectionRegistry,
                            TelemetryService telemetryService, ShadowService shadowService,
                            CommandService commandService, HttpCommandPublisher httpCommandPublisher) {
        this.deviceRegistry = deviceRegistry;
        this.connectionRegistry = connectionRegistry;
        this.telemetryService = telemetryService;
        this.shadowService = shadowService;
        this.commandService = commandService;
        this.httpCommandPublisher = httpCommandPublisher;
    }

    private String authenticate(String pathDeviceId, String headerDeviceId, String secret) {
        String deviceId = headerDeviceId != null ? headerDeviceId : pathDeviceId;
        if (headerDeviceId != null && !headerDeviceId.equals(pathDeviceId)) {
            throw new RejectException(ErrorCode.BAD_CREDENTIALS, "鉴权设备与路径设备不一致");
        }
        String error = deviceRegistry.checkCredentials(deviceId, secret);
        if (error != null) {
            throw new RejectException(ErrorCode.valueOf(error), "鉴权失败：" + error);
        }
        return deviceId;
    }

    @PostMapping("/connect")
    public Map<String, Object> connect(@PathVariable String deviceId,
                                       @RequestHeader(value = "X-Device-Id", required = false) String headerId,
                                       @RequestHeader(value = "X-Device-Secret", required = false) String secret) {
        String authenticated = authenticate(deviceId, headerId, secret);
        connectionRegistry.online(authenticated, "HTTP");
        commandService.onDeviceConnected(authenticated, "HTTP");
        return Map.of("deviceId", authenticated, "online", true);
    }

    @PostMapping("/disconnect")
    public Map<String, Object> disconnect(@PathVariable String deviceId,
                                          @RequestHeader(value = "X-Device-Id", required = false) String headerId,
                                          @RequestHeader(value = "X-Device-Secret", required = false) String secret) {
        String authenticated = authenticate(deviceId, headerId, secret);
        connectionRegistry.offline(authenticated, "HTTP", "设备主动断开");
        return Map.of("deviceId", authenticated, "online", false);
    }

    @PostMapping("/telemetry")
    public Map<String, Object> telemetry(@PathVariable String deviceId,
                                         @RequestHeader(value = "X-Device-Id", required = false) String headerId,
                                         @RequestHeader(value = "X-Device-Secret", required = false) String secret,
                                         @RequestBody(required = false) String rawBody) {
        String authenticated = authenticate(deviceId, headerId, secret);
        if (rawBody == null || rawBody.isBlank()) {
            throw new RejectException(ErrorCode.MALFORMED_PAYLOAD, "请求体为空");
        }
        boolean applied = telemetryService.ingest(authenticated, rawBody, "HTTP");
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("accepted", true);
        response.put("applied", applied);
        response.put("transport", "HTTP");
        if (!applied) {
            response.put("reason", "STALE_TELEMETRY");
        }
        return response;
    }

    @GetMapping("/shadow")
    public DeviceShadow shadow(@PathVariable String deviceId,
                               @RequestHeader(value = "X-Device-Id", required = false) String headerId,
                               @RequestHeader(value = "X-Device-Secret", required = false) String secret) {
        String authenticated = authenticate(deviceId, headerId, secret);
        return shadowService.getShadow(authenticated);
    }

    @PostMapping("/commands/{commandId}/ack")
    public Command acknowledge(@PathVariable String deviceId,
                               @PathVariable String commandId,
                               @RequestHeader(value = "X-Device-Id", required = false) String headerId,
                               @RequestHeader(value = "X-Device-Secret", required = false) String secret,
                               @RequestBody(required = false) Map<String, Object> body) {
        authenticate(deviceId, headerId, secret);
        String result = body == null ? "OK" : String.valueOf(body.getOrDefault("result", "OK"));
        return commandService.acknowledge(commandId, result);
    }

    @GetMapping("/commands/pull")
    public ResponseEntity<Command> pull(@PathVariable String deviceId,
                                        @RequestHeader(value = "X-Device-Id", required = false) String headerId,
                                        @RequestHeader(value = "X-Device-Secret", required = false) String secret,
                                        @RequestParam(value = "waitSeconds", defaultValue = "3") long waitSeconds) {
        String authenticated = authenticate(deviceId, headerId, secret);
        connectionRegistry.online(authenticated, "HTTP");

        CompletableFuture<Command> future = new CompletableFuture<>();
        httpCommandPublisher.registerPoll(authenticated, future);
        commandService.notifyHttpPoll(authenticated);
        try {
            Command command = future.get(Math.max(1, waitSeconds), TimeUnit.SECONDS);
            return ResponseEntity.ok(command);
        } catch (TimeoutException e) {
            return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
        }
    }
}
