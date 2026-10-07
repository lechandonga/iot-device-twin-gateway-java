package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayCore;
import com.github.lechandonga.iotdevicetwingatewayjava.core.IngestResult;
import com.github.lechandonga.iotdevicetwingatewayjava.core.TelemetryIngest;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 设备侧 HTTP 接入。与 MQTT 映射到同一套核心调用：
 * POST /api/device/{deviceId}/telemetry  <-> devices/{id}/telemetry
 * POST /api/device/{deviceId}/commands/{commandId}/ack <-> devices/{id}/cmd/ack
 * 凭据用 X-Device-Secret 请求头（对应 MQTT CONNECT 的 password）。
 */
@RestController
@RequestMapping("/api/device/{deviceId}")
public class DeviceHttpController {

    private static final Logger log = LoggerFactory.getLogger(DeviceHttpController.class);

    private final GatewayCore core;

    public DeviceHttpController(GatewayCore core) {
        this.core = core;
    }

    @PostMapping("/telemetry")
    public ResponseEntity<?> telemetry(@PathVariable String deviceId,
                                       @RequestHeader(value = "X-Device-Secret", required = false) String secret,
                                       @RequestBody(required = false) ApiDtos.TelemetryRequest body) {
        if (body == null || body.properties() == null) {
            // properties 缺失按稳定缺省语义处理为空对象，但整体 body 为空是畸形
            if (body == null) {
                return reject(HttpStatus.BAD_REQUEST, RejectReason.MALFORMED_PAYLOAD, "请求体为空");
            }
        }
        try {
            String payloadDeviceId = body.deviceId() != null ? body.deviceId() : deviceId;
            if (!deviceId.equals(payloadDeviceId)) {
                return reject(HttpStatus.BAD_REQUEST, RejectReason.MALFORMED_PAYLOAD,
                        "deviceId 与路径不一致");
            }
            TelemetryIngest ingest = new TelemetryIngest(deviceId, body.ts(), body.seq(),
                    body.properties() == null ? Map.of() : body.properties(), ProtocolType.HTTP);
            IngestResult result = core.ingest(deviceId, secret, ingest);
            return ResponseEntity.ok(Map.of(
                    "accepted", result.accepted(),
                    "reason", result.reason(),
                    "reportedSeq", result.shadowReportedSeq(),
                    "reportedTs", result.shadowReportedTs()));
        } catch (com.github.lechandonga.iotdevicetwingatewayjava.model.GatewayException ex) {
            return reject(mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
    }

    @PostMapping("/commands/{commandId}/ack")
    public ResponseEntity<?> ack(@PathVariable String deviceId,
                                 @PathVariable String commandId,
                                 @RequestHeader(value = "X-Device-Secret", required = false) String secret,
                                 @RequestBody(required = false) ApiDtos.AckRequest body) {
        if (body == null || body.commandId() == null || !body.commandId().equals(commandId)) {
            return reject(HttpStatus.BAD_REQUEST, RejectReason.MALFORMED_PAYLOAD,
                    "commandId 必填且须与路径一致");
        }
        try {
            Command cmd = core.ackCommand(deviceId, secret, commandId,
                    body.result(), body.reported());
            log.info("[HTTP] 命令确认 device={} commandId={} status={}", deviceId, commandId, cmd.status());
            return ResponseEntity.ok(CommandViews.of(cmd));
        } catch (com.github.lechandonga.iotdevicetwingatewayjava.model.GatewayException ex) {
            return reject(mapStatus(ex.reason()), ex.reason(), ex.getMessage());
        }
    }

    static HttpStatus mapStatus(RejectReason reason) {
        return switch (reason) {
            case DEVICE_NOT_REGISTERED -> HttpStatus.NOT_FOUND;
            case BAD_CREDENTIALS, UNAUTHORIZED -> HttpStatus.UNAUTHORIZED;
            case DEVICE_OVERLOADED, GATEWAY_OVERLOADED -> HttpStatus.SERVICE_UNAVAILABLE;
            case CONFLICT -> HttpStatus.CONFLICT;
            case UNKNOWN_COMMAND -> HttpStatus.NOT_FOUND;
            case MALFORMED_PAYLOAD -> HttpStatus.BAD_REQUEST;
        };
    }

    static ResponseEntity<ApiDtos.ErrorResponse> reject(HttpStatus status, RejectReason reason, String message) {
        return ResponseEntity.status(status)
                .body(new ApiDtos.ErrorResponse(true, reason.code(), reason.description() + ": " + message));
    }
}
