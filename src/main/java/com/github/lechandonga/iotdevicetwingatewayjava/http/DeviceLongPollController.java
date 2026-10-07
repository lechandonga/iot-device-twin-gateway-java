package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 设备侧命令长轮询：GET /api/device/{id}/commands?waitMs=...
 * 第一次调用建立 HTTP 会话（在线），持续轮询期间在线；
 * 停止轮询超过 keepAlive 视为离线（由超时扫描判定）。
 */
@RestController
@RequestMapping("/api/device/{deviceId}/commands")
public class DeviceLongPollController {

    private static final Logger log = LoggerFactory.getLogger(DeviceLongPollController.class);

    private final HttpSessionRegistry registry;
    private final GatewayCoreProvider coreProvider;

    public DeviceLongPollController(HttpSessionRegistry registry, GatewayCoreProvider coreProvider) {
        this.registry = registry;
        this.coreProvider = coreProvider;
    }

    @GetMapping
    public ResponseEntity<?> poll(@PathVariable String deviceId,
                                  @RequestHeader(value = "X-Device-Secret", required = false) String secret,
                                  @RequestParam(value = "waitMs", defaultValue = "1000") long waitMs) {
        var core = coreProvider.core();
        // 鉴权（未注册/凭据错误可区分），同时建立/刷新在线会话
        HttpDeviceSession session = registry.getOrOpen(deviceId);
        try {
            core.connect(deviceId, secret, session);
        } catch (com.github.lechandonga.iotdevicetwingatewayjava.model.GatewayException ex) {
            return DeviceHttpController.reject(DeviceHttpController.mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
        try {
            Command command = session.poll(Math.min(waitMs, 10_000));
            if (command == null) {
                return ResponseEntity.ok(Map.of("commands", java.util.List.of()));
            }
            Map<String, Object> view = new LinkedHashMap<>(CommandViews.of(command));
            return ResponseEntity.ok(Map.of("commands", java.util.List.of(view)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
    }

    @org.springframework.web.bind.annotation.DeleteMapping
    public ResponseEntity<?> goOffline(@PathVariable String deviceId,
                                       @RequestHeader(value = "X-Device-Secret", required = false) String secret) {
        var core = coreProvider.core();
        try {
            core.authenticate(deviceId, secret);
        } catch (com.github.lechandonga.iotdevicetwingatewayjava.model.GatewayException ex) {
            return DeviceHttpController.reject(DeviceHttpController.mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
        registry.close(deviceId);
        core.disconnect(deviceId, ProtocolType.HTTP);
        log.info("[HTTP] 设备主动下线 device={}", deviceId);
        return ResponseEntity.ok(Map.of("state", "OFFLINE"));
    }
}
