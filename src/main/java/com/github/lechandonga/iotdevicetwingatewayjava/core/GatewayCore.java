package com.github.lechandonga.iotdevicetwingatewayjava.core;

import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceCredential;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceShadow;
import com.github.lechandonga.iotdevicetwingatewayjava.model.GatewayException;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectReason;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 网关核心门面：设备注册、凭据校验、遥测归一化、影子与命令状态机。
 * MQTT 适配层与 HTTP 适配层只调用本类，因此两种接入方式的业务结论完全一致。
 */
@Component
public class GatewayCore {

    private final GatewayProperties props;
    private final Clock clock;
    private final Map<String, DeviceActor> devices = new ConcurrentHashMap<>();
    private final Semaphore globalInflight;
    private ScheduledExecutorService sweeper;

    public GatewayCore(GatewayProperties props, Clock clock) {
        this.props = props;
        this.clock = clock;
        this.globalInflight = new Semaphore(props.getGlobalInflightLimit(), true);
    }

    @PostConstruct
    void startSweeper() {
        sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "command-timeout-sweeper");
            t.setDaemon(true);
            return t;
        });
        sweeper.scheduleAtFixedRate(this::sweepAll,
                props.getTimeoutSweepIntervalMs(), props.getTimeoutSweepIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stopSweeper() {
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
    }

    private void sweepAll() {
        for (DeviceActor actor : devices.values()) {
            try {
                actor.sweepTimeouts();
            } catch (RuntimeException ignored) {
                // 单个设备扫描异常不影响其他设备
            }
        }
    }

    public synchronized DeviceCredential register(String deviceId, String secret) {
        if (devices.containsKey(deviceId)) {
            throw new GatewayException(RejectReason.CONFLICT);
        }
        DeviceCredential credential = new DeviceCredential(deviceId, secret, Instant.now(clock));
        devices.put(deviceId, new DeviceActor(credential, props, clock, globalInflight));
        return credential;
    }

    public boolean isRegistered(String deviceId) {
        return devices.containsKey(deviceId);
    }

    /** 设备侧鉴权：未注册与凭据错误给出可区分原因。 */
    public DeviceActor authenticate(String deviceId, String secret) {
        DeviceActor actor = devices.get(deviceId);
        if (actor == null) {
            throw new GatewayException(RejectReason.DEVICE_NOT_REGISTERED);
        }
        actor.checkSecret(secret);
        return actor;
    }

    /** 北向管理接口鉴权。 */
    public void requireAdmin(String token) {
        if (token == null || !props.getAdminToken().equals(token)) {
            throw new GatewayException(RejectReason.UNAUTHORIZED);
        }
    }

    public void connect(String deviceId, String secret, DeviceSession session) {
        authenticate(deviceId, secret).onConnect(session);
    }

    public void disconnect(String deviceId, com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType protocol) {
        DeviceActor actor = devices.get(deviceId);
        if (actor != null) {
            actor.onDisconnect(protocol);
        }
    }

    public IngestResult ingest(String deviceId, String secret, TelemetryIngest ingest) {
        return authenticate(deviceId, secret).ingest(ingest);
    }

    public Command sendCommand(String deviceId, String name, Map<String, Object> params,
                               Map<String, Object> desiredPatch) {
        DeviceActor actor = devices.get(deviceId);
        if (actor == null) {
            throw new GatewayException(RejectReason.DEVICE_NOT_REGISTERED);
        }
        return actor.enqueueCommand(UUID.randomUUID().toString(), name, params, desiredPatch);
    }

    public Command ackCommand(String deviceId, String secret, String commandId,
                              Map<String, Object> result, Map<String, Object> reportedPatch) {
        return authenticate(deviceId, secret).ackCommand(commandId, result, reportedPatch);
    }

    public DeviceShadow shadow(String deviceId) {
        DeviceActor actor = devices.get(deviceId);
        if (actor == null) {
            throw new GatewayException(RejectReason.DEVICE_NOT_REGISTERED);
        }
        return actor.shadow();
    }

    public DeviceShadow updateDesired(String deviceId, Map<String, Object> patch) {
        DeviceActor actor = devices.get(deviceId);
        if (actor == null) {
            throw new GatewayException(RejectReason.DEVICE_NOT_REGISTERED);
        }
        return actor.updateDesired(patch);
    }

    public List<Command> commandHistory(String deviceId) {
        DeviceActor actor = devices.get(deviceId);
        if (actor == null) {
            throw new GatewayException(RejectReason.DEVICE_NOT_REGISTERED);
        }
        return actor.commandHistory();
    }

    public Optional<Command> command(String deviceId, String commandId) {
        DeviceActor actor = devices.get(deviceId);
        if (actor == null) {
            throw new GatewayException(RejectReason.DEVICE_NOT_REGISTERED);
        }
        return actor.command(commandId);
    }
}
