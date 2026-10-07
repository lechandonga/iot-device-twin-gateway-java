package com.github.lechandonga.iotdevicetwingatewayjava.tests;

import com.github.lechandonga.iotdevicetwingatewayjava.core.DeviceSession;
import com.github.lechandonga.iotdevicetwingatewayjava.core.GatewayProperties;
import com.github.lechandonga.iotdevicetwingatewayjava.core.TelemetryIngest;
import com.github.lechandonga.iotdevicetwingatewayjava.core.DeviceActor;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceCredential;
import com.github.lechandonga.iotdevicetwingatewayjava.model.GatewayException;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.assertEquals;
import static com.github.lechandonga.iotdevicetwingatewayjava.tests.TestFixtures.assertTrue;

/**
 * 核心层容量与故障隔离测试（不启动 Spring）：
 * 1) 单设备处理变慢导致队列打满时，新请求明确收到 DEVICE_OVERLOADED，而不是悄悄排队堆积；
 * 2) 一个设备被打满/阻塞时，另一个设备的接入完全不受影响；
 * 3) 全局容量耗尽时返回 GATEWAY_OVERLOADED。
 */
class OverloadIsolationTests {

    private static final Logger log = LoggerFactory.getLogger("容量隔离");

    private final GatewayProperties props = new GatewayProperties();

    private DeviceActor actor(String id, int perDeviceCapacity, Semaphore global) {
        props.setPerDeviceQueueCapacity(perDeviceCapacity);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000_000L), ZoneOffset.UTC);
        return new DeviceActor(new DeviceCredential(id, "s", Instant.now(clock)), props, clock, global);
    }

    @Test
    void overloadedDeviceIsRejectedAndOthersUnaffected() throws Exception {
        log.info("==== 场景：单设备过载明确拒绝，其他设备不受影响 ====");
        Semaphore global = new Semaphore(1000);
        DeviceActor slow = actor("slow", 2, global);
        DeviceActor healthy = actor("healthy", 2, global);

        global.acquireUninterruptibly(1000); // 耗尽全局容量
        try {
            healthy.ingest(new TelemetryIngest("healthy", 1L, 1L, Map.of("a", 1), ProtocolType.HTTP));
            throw new AssertionError("网关容量耗尽时必须拒绝");
        } catch (GatewayException e) {
            assertEquals(RejectReason.GATEWAY_OVERLOADED, e.reason(), "全局容量耗尽 -> GATEWAY_OVERLOADED");
        } finally {
            global.release(1000);
        }

        // 全局恢复后，healthy 正常工作（未受 slow 影响）
        var result = healthy.ingest(
                new TelemetryIngest("healthy", 1L, 1L, Map.of("a", 1), ProtocolType.HTTP));
        assertTrue(result.accepted(), "容量恢复后其他设备立即恢复接入", result.reason());

        // 设备级队列满：构造一个投递命令会长时间阻塞的会话，灌命令时队列被占满
        DeviceActor bounded = actor("bounded", 1, new Semaphore(1000));
        DeviceSession hangingSession = new DeviceSession() {
            @Override
            public ProtocolType protocol() {
                return ProtocolType.MQTT;
            }

            @Override
            public void deliver(com.github.lechandonga.iotdevicetwingatewayjava.model.Command command)
                    throws TransportException {
                try {
                    Thread.sleep(3_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        bounded.onConnect(hangingSession);
        // 第一条命令开始投递并阻塞在 actor 线程；第二条命令进入容量为 1 的队列；
        // 之后的 enqueue 立即被拒绝 DEVICE_OVERLOADED
        Thread t1 = new Thread(() -> bounded.enqueueCommand("c1", "n", Map.of(), Map.of()));
        t1.setDaemon(true);
        t1.start();
        Thread.sleep(300);
        Thread t2 = new Thread(() -> bounded.enqueueCommand("c2", "n", Map.of(), Map.of()));
        t2.setDaemon(true);
        t2.start();
        Thread.sleep(300);

        boolean sawDeviceOverload = false;
        for (int i = 0; i < 20; i++) {
            try {
                bounded.enqueueCommand("c" + (100 + i), "n", Map.of(), Map.of());
            } catch (GatewayException e) {
                if (e.reason() == RejectReason.DEVICE_OVERLOADED) {
                    sawDeviceOverload = true;
                    break;
                }
            }
        }
        assertTrue(sawDeviceOverload, "队列满返回 DEVICE_OVERLOADED，不悄悄堆积", "已观察到拒绝");

        // bounded 设备打满期间，healthy 仍然可用
        var healthyResult = healthy.ingest(
                new TelemetryIngest("healthy", 2L, 2L, Map.of("a", 2), ProtocolType.MQTT));
        assertTrue(healthyResult.accepted(), "故障设备不影响其他设备接入", healthyResult.reason());

        t1.interrupt();
        t2.interrupt();
    }

    @Test
    void defaultingSemanticsAreStable() {
        log.info("==== 场景：字段缺省语义稳定（两种协议共用核心） ====");
        DeviceActor a = actor("d", 64, new Semaphore(1000));
        var first = a.ingest(new TelemetryIngest("d", null, null, null, ProtocolType.MQTT));
        assertTrue(first.accepted(), "ts/seq/properties 全缺省仍可接受", first.reason());
        assertEquals(0L, first.shadowReportedSeq(), "无历史时派生 seq=0");
        var second = a.ingest(new TelemetryIngest("d", null, 5L, null, ProtocolType.HTTP));
        var third = a.ingest(new TelemetryIngest("d", null, null, Map.of("x", 1), ProtocolType.HTTP));
        assertEquals(6L, third.shadowReportedSeq(), "再次缺省 seq 派生为 5+1=6");
        assertTrue(second.accepted() && third.accepted(), "缺省 properties 视为空对象", "OK");
    }
}
