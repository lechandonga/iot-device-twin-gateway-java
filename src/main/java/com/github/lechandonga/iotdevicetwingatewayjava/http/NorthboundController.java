package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayCore;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceCredential;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceShadow;
import com.github.lechandonga.iotdevicetwingatewayjava.model.GatewayException;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 北向（云平台/应用侧）管理接口。统一使用 X-Admin-Token 鉴权。
 */
@RestController
@RequestMapping("/api/admin")
public class NorthboundController {

    private static final Logger log = LoggerFactory.getLogger(NorthboundController.class);

    private final GatewayCore core;

    public NorthboundController(GatewayCore core) {
        this.core = core;
    }

    @PostMapping("/devices/{deviceId}/register")
    public ResponseEntity<?> register(@RequestHeader(value = "X-Admin-Token", required = false) String token,
                                      @PathVariable String deviceId,
                                      @RequestBody(required = false) ApiDtos.RegisterRequest body) {
        try {
            core.requireAdmin(token);
            if (body == null || body.secret() == null || body.secret().isBlank()) {
                return DeviceHttpController.reject(HttpStatus.BAD_REQUEST, RejectReason.MALFORMED_PAYLOAD,
                        "secret 必填");
            }
            DeviceCredential credential = core.register(deviceId, body.secret());
            log.info("[北向] 注册设备 device={}", deviceId);
            return ResponseEntity.ok(Map.of("deviceId", credential.deviceId(),
                    "registeredAt", credential.registeredAt().toEpochMilli()));
        } catch (GatewayException ex) {
            return DeviceHttpController.reject(DeviceHttpController.mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
    }

    @PostMapping("/devices/{deviceId}/commands")
    public ResponseEntity<?> sendCommand(@RequestHeader(value = "X-Admin-Token", required = false) String token,
                                         @PathVariable String deviceId,
                                         @RequestBody(required = false) ApiDtos.CommandRequest body) {
        try {
            core.requireAdmin(token);
            if (body == null || body.name() == null || body.name().isBlank()) {
                return DeviceHttpController.reject(HttpStatus.BAD_REQUEST, RejectReason.MALFORMED_PAYLOAD,
                        "name 必填");
            }
            Command command = core.sendCommand(deviceId, body.name(), body.params(), body.desired());
            log.info("[北向] 下发命令 device={} commandId={} status={}",
                    deviceId, command.commandId(), command.status());
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(CommandViews.of(command));
        } catch (GatewayException ex) {
            return DeviceHttpController.reject(DeviceHttpController.mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
    }

    @GetMapping("/devices/{deviceId}/commands")
    public ResponseEntity<?> commandHistory(@RequestHeader(value = "X-Admin-Token", required = false) String token,
                                            @PathVariable String deviceId) {
        try {
            core.requireAdmin(token);
            List<Map<String, Object>> history = core.commandHistory(deviceId).stream()
                    .map(CommandViews::of).toList();
            return ResponseEntity.ok(Map.of("deviceId", deviceId, "commands", history));
        } catch (GatewayException ex) {
            return DeviceHttpController.reject(DeviceHttpController.mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
    }

    @GetMapping("/devices/{deviceId}/commands/{commandId}")
    public ResponseEntity<?> command(@RequestHeader(value = "X-Admin-Token", required = false) String token,
                                     @PathVariable String deviceId,
                                     @PathVariable String commandId) {
        try {
            core.requireAdmin(token);
            return core.command(deviceId, commandId)
                    .<ResponseEntity<?>>map(cmd -> ResponseEntity.ok(CommandViews.of(cmd)))
                    .orElseGet(() -> DeviceHttpController.reject(HttpStatus.NOT_FOUND,
                            RejectReason.UNKNOWN_COMMAND, commandId));
        } catch (GatewayException ex) {
            return DeviceHttpController.reject(DeviceHttpController.mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
    }

    @GetMapping("/devices/{deviceId}/shadow")
    public ResponseEntity<?> shadow(@RequestHeader(value = "X-Admin-Token", required = false) String token,
                                    @PathVariable String deviceId) {
        try {
            core.requireAdmin(token);
            DeviceShadow shadow = core.shadow(deviceId);
            return ResponseEntity.ok(shadowView(shadow));
        } catch (GatewayException ex) {
            return DeviceHttpController.reject(DeviceHttpController.mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
    }

    @PostMapping("/devices/{deviceId}/desired")
    public ResponseEntity<?> updateDesired(@RequestHeader(value = "X-Admin-Token", required = false) String token,
                                           @PathVariable String deviceId,
                                           @RequestBody(required = false) ApiDtos.DesiredRequest body) {
        try {
            core.requireAdmin(token);
            if (body == null || body.desired() == null || body.desired().isEmpty()) {
                return DeviceHttpController.reject(HttpStatus.BAD_REQUEST, RejectReason.MALFORMED_PAYLOAD,
                        "desired 必填且非空");
            }
            DeviceShadow shadow = core.updateDesired(deviceId, body.desired());
            return ResponseEntity.ok(shadowView(shadow));
        } catch (GatewayException ex) {
            return DeviceHttpController.reject(DeviceHttpController.mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
    }

    static Map<String, Object> shadowView(DeviceShadow s) {
        return Map.of(
                "deviceId", s.deviceId(),
                "connectionState", s.connectionState(),
                "desired", s.desired(),
                "reported", s.reported(),
                "desiredVersion", s.desiredVersion(),
                "reportedSeq", s.reportedSeq(),
                "reportedTs", s.reportedTs(),
                "syncState", s.syncState(),
                "delta", s.delta());
    }
}
