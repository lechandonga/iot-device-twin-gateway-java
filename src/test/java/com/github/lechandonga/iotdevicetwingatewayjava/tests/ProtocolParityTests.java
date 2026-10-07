package com.github.lechandonga.iotdevicetwingatewayjava.tests;

import com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayCore;
import com.github.lechandonga.iotdevicetwingatewayjava.core.IngestResult;
import com.github.lechandonga.iotdevicetwingatewayjava.core.TelemetryIngest;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceShadow;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.assertEquals;
import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.assertTrue;

/**
 * 协议一致性：同样的遥测序列分别经 MQTT 与 HTTP 归一化进入核心后，
 * 影子（除协议来源标记外）结论完全一致。
 */
@SpringBootTest
class ProtocolParityTests {

    private static final Logger log = LoggerFactory.getLogger("协议一致性");

    @Autowired
    private GatewayCore core;

    @Test
    void mqttAndHttpProduceIdenticalShadow() {
        log.info("==== 场景：MQTT 与 HTTP 归一化结论一致 ====");
        core.register("p-mqtt", "s");
        core.register("p-http", "s");

        TelemetryIngest[] sequence = {
                new TelemetryIngest(null, 100L, 1L, Map.of("temp", 20, "hum", 50), null),
                new TelemetryIngest(null, 200L, 2L, Map.of("temp", 22), null),
                new TelemetryIngest(null, 300L, 3L, Map.of("temp", 21), null), // 更新的 seq=3
                new TelemetryIngest(null, 50L, 2L, Map.of("temp", 99), null),  // 乱序，必须忽略
                new TelemetryIngest(null, 250L, 4L, Map.of("hum", 55), null),
        };

        DeviceShadow mqttShadow = replay("p-mqtt", "s", ProtocolType.MQTT, sequence);
        DeviceShadow httpShadow = replay("p-http", "s", ProtocolType.HTTP, sequence);

        assertEquals(mqttShadow.reported(), httpShadow.reported(), "两协议 reported 完全一致");
        assertEquals(mqttShadow.reportedSeq(), httpShadow.reportedSeq(), "两协议 reportedSeq 一致");
        assertEquals(mqttShadow.reportedTs(), httpShadow.reportedTs(), "两协议 reportedTs 一致");
        assertEquals(21, mqttShadow.reported().get("temp"), "乱序的 temp=99 未覆盖");
        assertEquals(55, mqttShadow.reported().get("hum"), "缺省报文的 hum=55 被接受");
        assertTrue(mqttShadow.reported().get("temp").equals(httpShadow.reported().get("temp")),
                "关键业务字段在两种接入下结论一致", "temp 均为 21");
    }

    private DeviceShadow replay(String id, String secret, ProtocolType protocol, TelemetryIngest... seq) {
        for (TelemetryIngest raw : seq) {
            IngestResult result = core.ingest(id, secret,
                    new TelemetryIngest(id, raw.tsEpochMillis(), raw.seq(), raw.properties(), protocol));
            assertTrue(result.accepted() || result.reason().equals(IngestResult.STALE),
                    "每个报文要么接受要么被旧数据保护忽略", result.reason());
        }
        return core.shadow(id);
    }
}
