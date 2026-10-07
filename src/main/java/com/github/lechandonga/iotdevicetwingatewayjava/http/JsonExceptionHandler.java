package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectReason;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** JSON 解析失败统一归为 MALFORMED_PAYLOAD，与 MQTT 的畸形报文拒绝原因一致。 */
@RestControllerAdvice
public class JsonExceptionHandler {

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiDtos.ErrorResponse> malformed(HttpMessageNotReadableException ex) {
        return DeviceHttpController.reject(HttpStatus.BAD_REQUEST, RejectReason.MALFORMED_PAYLOAD,
                "请求体不是合法 JSON");
    }
}
