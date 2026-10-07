package com.github.lechandonga.iotdevicetwingatewayjava.core;

import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceShadow;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ErrorCode;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectException;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Telemetry;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.ConnectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备影子服务。
 * 规则：
 * - reported 只接受 ts 严格大于已采纳时间戳的遥测（乱序/迟到不回退）
 * - desired 由云端显式设置，版本号单调递增
 * - desired 全部键被 reported 以相等值覆盖时为 IN_SYNC，否则 OUT_OF_SYNC
 */
@Service
public class ShadowService {

    private static final Logger log = LoggerFactory.getLogger(ShadowService.class);

    private static final class ShadowState {
        long lastTs = Long.MIN_VALUE;
        final Map<String, Object> reported = new LinkedHashMap<>();
        final Map<String, Object> desired = new LinkedHashMap<>();
        long version;
    }

    private final Map<String, ShadowState> shadows = new ConcurrentHashMap<>();
    private final DeviceLockManager lockManager;
    private final ConnectionRegistry connectionRegistry;

    public ShadowService(DeviceLockManager lockManager, ConnectionRegistry connectionRegistry) {
        this.lockManager = lockManager;
        this.connectionRegistry = connectionRegistry;
    }

    /**
     * 应用遥测到影子 reported。
     */
    public boolean applyTelemetry(Telemetry telemetry) {
        var lock = lockManager.lockFor(telemetry.deviceId());
        lock.lock();
        try {
            ShadowState state = shadows.computeIfAbsent(telemetry.deviceId(), key -> new ShadowState());
            if (telemetry.ts() <= state.lastTs) {
                return false;
            }
            state.lastTs = telemetry.ts();
            state.reported.putAll(telemetry.properties());
            state.version++;
            logShadowSync(telemetry.deviceId(), state);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 云端设置期望值（合并语义）。
     */
    public DeviceShadow setDesired(String deviceId, Map<String, Object> patch) {
        var lock = lockManager.lockFor(deviceId);
        lock.lock();
        try {
            ShadowState state = shadows.computeIfAbsent(deviceId, key -> new ShadowState());
            state.desired.putAll(patch);
            state.version++;
            logShadowSync(deviceId, state);
            return snapshot(deviceId, state);
        } finally {
            lock.unlock();
        }
    }

    public DeviceShadow getShadow(String deviceId) {
        var lock = lockManager.lockFor(deviceId);
        lock.lock();
        try {
            ShadowState state = shadows.get(deviceId);
            if (state == null) {
                throw new RejectException(ErrorCode.DEVICE_NOT_REGISTERED,
                        "设备 " + deviceId + " 没有影子（可能未注册）");
            }
            return snapshot(deviceId, state);
        } finally {
            lock.unlock();
        }
    }

    private DeviceShadow snapshot(String deviceId, ShadowState state) {
        return new DeviceShadow(
                deviceId,
                connectionRegistry.isOnline(deviceId),
                state.lastTs == Long.MIN_VALUE ? 0 : state.lastTs,
                Map.copyOf(state.reported),
                Map.copyOf(state.desired),
                computeSyncState(state),
                state.version
        );
    }

    private String computeSyncState(ShadowState state) {
        if (state.desired.isEmpty()) {
            return "IN_SYNC";
        }
        for (Map.Entry<String, Object> entry : state.desired.entrySet()) {
            Object reportedValue = state.reported.get(entry.getKey());
            if (!Objects.equals(reportedValue, entry.getValue())) {
                return "OUT_OF_SYNC";
            }
        }
        return "IN_SYNC";
    }

    private void logShadowSync(String deviceId, ShadowState state) {
        log.info("[影子] 设备 {} 同步状态={} reported={} desired={}",
                deviceId, computeSyncState(state), state.reported, state.desired);
    }

    public void clear() {
        shadows.clear();
    }
}
