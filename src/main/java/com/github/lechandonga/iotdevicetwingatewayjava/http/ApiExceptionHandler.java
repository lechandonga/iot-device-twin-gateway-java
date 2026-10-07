package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.model.ErrorCode;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(RejectException.class)
    public ResponseEntity<Map<String, Object>> handleReject(RejectException exception) {
        HttpStatus status = switch (exception.errorCode()) {
            case MISSING_CREDENTIALS, BAD_CREDENTIALS -> HttpStatus.UNAUTHORIZED;
            case DEVICE_NOT_REGISTERED -> HttpStatus.FORBIDDEN;
            case COMMAND_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case GATEWAY_BUSY, COMMAND_QUEUE_FULL -> HttpStatus.SERVICE_UNAVAILABLE;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status).body(Map.of(
                "accepted", false,
                "errorCode", exception.errorCode().name(),
                "message", exception.getMessage()
        ));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleUnexpected(Exception exception) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "accepted", false,
                "errorCode", ErrorCode.MALFORMED_PAYLOAD.name(),
                "message", String.valueOf(exception.getMessage())
        ));
    }
}
