package com.github.lechandonga.iotdevicetwingatewayjava.http;

import java.util.Map;

/** REST 接口的请求/响应 DTO。 */
public final class ApiDtos {

    private ApiDtos() {
    }

    public record RegisterRequest(String secret) {}

    public record TelemetryRequest(String deviceId, Long ts, Long seq, Map<String, Object> properties) {}

    public record AckRequest(String commandId, Map<String, Object> result, Map<String, Object> reported) {}

    public record CommandRequest(String name, Map<String, Object> params, Map<String, Object> desired) {}

    public record DesiredRequest(Map<String, Object> desired) {}

    public record ErrorResponse(boolean rejected, String reason, String message) {}
}
