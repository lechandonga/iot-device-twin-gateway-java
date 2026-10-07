package com.github.lechandonga.iotdevicetwingatewayjava.http;

import com.fasterxml.jackson.databind.ObjectMapper;

/** 协议适配层共用 JSON 解析，保证两种接入方式对畸形报文的判定一致。 */
public final class JsonSupport {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonSupport() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }
}
