package com.github.lechandonga.iotdevicetwingatewayjava.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ErrorCode;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectException;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Telemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 遥测归一化：MQTT 报文与 HTTP 报文在这里合并为同一模型。
 *
 * 缺省语义（稳定且明确）：
 * - ts 缺失或 null：使用网关接收时间
 * - properties 缺失或 null：视为空属性对象
 * - 报文不是 JSON 对象、ts 非数字、properties 不是对象：MALFORMED_PAYLOAD
 */
@Service
public class TelemetryService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryService.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final IngestGate ingestGate;
    private final ShadowService shadowService;
    private final GatewayProperties properties;

    public TelemetryService(GatewayProperties properties, ShadowService shadowService) {
        this.properties = properties;
        this.ingestGate = new IngestGate(properties.getCapacity());
        this.shadowService = shadowService;
    }

    /**
     * 归一化原始 JSON 文本。
     */
    public Telemetry normalize(String deviceId, String rawJson, String transport) {
        JsonNode root;
        try {
            root = objectMapper.readTree(rawJson);
        } catch (Exception e) {
            throw new RejectException(ErrorCode.MALFORMED_PAYLOAD, "报文不是合法 JSON");
        }
        if (root == null || !root.isObject()) {
            throw new RejectException(ErrorCode.MALFORMED_PAYLOAD, "报文根节点必须是 JSON 对象");
        }

        long ts;
        JsonNode tsNode = root.get("ts");
        if (tsNode == null || tsNode.isNull()) {
            ts = System.currentTimeMillis();
        } else if (tsNode.isNumber()) {
            ts = tsNode.asLong();
        } else {
            throw new RejectException(ErrorCode.MALFORMED_PAYLOAD, "ts 必须是数字时间戳（毫秒）");
        }

        Map<String, Object> properties = new LinkedHashMap<>();
        JsonNode propsNode = root.get("properties");
        if (propsNode != null && !propsNode.isNull()) {
            if (!propsNode.isObject()) {
                throw new RejectException(ErrorCode.MALFORMED_PAYLOAD, "properties 必须是 JSON 对象");
            }
            propsNode.fields().forEachRemaining(entry -> {
                JsonNode value = entry.getValue();
                if (value.isContainerNode() || value.isNull()) {
                    throw new RejectException(ErrorCode.MALFORMED_PAYLOAD,
                            "properties." + entry.getKey() + " 必须是标量值");
                }
                properties.put(entry.getKey(), objectMapper.convertValue(value, Object.class));
            });
        }
        return new Telemetry(deviceId, ts, properties, transport);
    }

    /**
     * 应用归一化遥测。受全局容量闸门保护。
     *
     * @return true=已更新影子；false=乱序/迟到，被忽略
     */
    public boolean ingest(String deviceId, String rawJson, String transport) {
        Telemetry telemetry = normalize(deviceId, rawJson, transport);
            try (AutoCloseable ignored = ingestGate.acquire()) {
                if (properties.getProcessingDelayMs() > 0) {
                    Thread.sleep(properties.getProcessingDelayMs());
                }
                boolean applied = shadowService.applyTelemetry(telemetry);
            if (applied) {
                log.info("[遥测] 设备 {} 经 {} 上报已采纳 ts={} 属性={}",
                        deviceId, transport, telemetry.ts(), telemetry.properties());
            } else {
                log.warn("[遥测] 设备 {} 经 {} 的迟到/乱序上报被忽略 ts={}（未覆盖更新鲜状态）",
                        deviceId, transport, telemetry.ts());
            }
            return applied;
        } catch (RejectException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
