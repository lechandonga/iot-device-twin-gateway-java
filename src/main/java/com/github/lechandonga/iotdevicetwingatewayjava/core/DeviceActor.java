package com.github.lechandonga.iotdevicetwingatewayjava.core;

import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.CommandStatus;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ConnectionState;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceCredential;
import com.github.lechandonga.iotdevicetwingatewayjava.model.DeviceShadow;
import com.github.lechandonga.iotdevicetwingatewayjava.model.GatewayException;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ProtocolType;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectReason;
import com.github.lechandonga.iotdevicetwingatewayjava.model.SyncState;
import com.github.lechandonga.iotdevicetwingatewayjava.model.Telemetry;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 单设备串行执行体（actor 模式）。
 *
 * 所有状态变更只在该设备自己的单线程上发生：同一设备的并发遥测/命令不会丢更新、
 * 影子不会回退。队列有界且采用 Abort 拒绝策略，过载时显式拒绝而不是悄悄堆积。
 * 设备之间线程相互独立，单设备阻塞不影响其他设备接入。
 */
public class DeviceActor {

    private final DeviceCredential credential;
    private final GatewayProperties props;
    private final Clock clock;
    private final Semaphore globalInflight;
    private final ThreadPoolExecutor executor;

    private DeviceSession session;
    private ConnectionState connectionState = ConnectionState.OFFLINE;
    private final Map<String, Object> desired = new LinkedHashMap<>();
    private final Map<String, Object> reported = new LinkedHashMap<>();
    private long desiredVersion;
    private long reportedTs;
    private long reportedSeq = -1L;
    private boolean anyTelemetry;
    private final Deque<String> pendingQueue = new ArrayDeque<>();
    private final Map<String, Command> commands = new LinkedHashMap<>();

    public DeviceActor(DeviceCredential credential, GatewayProperties props, Clock clock, Semaphore globalInflight) {
        this.credential = credential;
        this.props = props;
        this.clock = clock;
        this.globalInflight = globalInflight;
        this.executor = new ThreadPoolExecutor(1, 1, 30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(props.getPerDeviceQueueCapacity()),
                r -> {
                    Thread t = new Thread(r, "device-actor-" + credential.deviceId());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    public String deviceId() {
        return credential.deviceId();
    }

    public void checkSecret(String secret) {
        if (!credential.secret().equals(secret)) {
            throw new GatewayException(RejectReason.BAD_CREDENTIALS);
        }
    }

    /**
     * 有序串行执行：调用方等待结果（保持调用顺序），但状态变更始终只发生在 actor 线程。
     * 设备队列满 -> DEVICE_OVERLOADED；全局容量满 -> GATEWAY_OVERLOADED。
     */
    private <T> T call(String taskLabel, java.util.function.Supplier<T> work) {
        if (!globalInflight.tryAcquire()) {
            throw new GatewayException(RejectReason.GATEWAY_OVERLOADED);
        }
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                try {
                    future.complete(work.get());
                } catch (RuntimeException ex) {
                    future.completeExceptionally(ex);
                } finally {
                    globalInflight.release();
                }
            });
        } catch (RuntimeException rejected) {
            globalInflight.release();
            throw new GatewayException(RejectReason.DEVICE_OVERLOADED);
        }
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GatewayException(RejectReason.GATEWAY_OVERLOADED);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(cause);
        }
    }

    private void run(String taskLabel, Runnable work) {
        call(taskLabel, () -> { work.run(); return null; });
    }

    // ---------------- 连接事件 ----------------

    public void onConnect(DeviceSession newSession) {
        run("connect", () -> {
            this.session = newSession;
            this.connectionState = ConnectionState.ONLINE;
            drainQueuedCommands();
        });
    }

    public void onDisconnect(ProtocolType protocol) {
        run("disconnect", () -> {
            if (session != null && session.protocol() != protocol) {
                return; // 另一协议的会话仍在线
            }
            this.session = null;
            this.connectionState = ConnectionState.OFFLINE;
            // 在途命令转回 QUEUED：重连后按原顺序补齐，不静默丢弃
            for (Map.Entry<String, Command> e : commands.entrySet()) {
                Command cmd = e.getValue();
                if (cmd.status() == CommandStatus.IN_FLIGHT) {
                    commands.put(e.getKey(), new Command(cmd.commandId(), cmd.deviceId(), cmd.name(),
                            cmd.params(), CommandStatus.QUEUED, cmd.attempts(), cmd.maxAttempts(),
                            cmd.createdAt(), cmd.lastDispatchedAt(), cmd.ackedAt(), cmd.ackResult(),
                            "connection lost before ack"));
                    if (!pendingQueue.contains(e.getKey())) {
                        pendingQueue.addLast(e.getKey());
                    }
                }
            }
        });
    }

    // ---------------- 遥测 ----------------

    public IngestResult ingest(TelemetryIngest raw) {
        return call("telemetry", () -> {
            long now = clock.millis();
            // 归一化在核心层完成，MQTT 与 HTTP 走同一段逻辑 => 缺省语义一致
            long ts = raw.tsEpochMillis() != null ? raw.tsEpochMillis() : now;
            long seq = raw.seq() != null ? raw.seq() : Math.max(reportedSeq + 1, 0);
            Map<String, Object> propsMap = raw.properties() == null
                    ? Map.of() : Map.copyOf(new LinkedHashMap<>(raw.properties()));
            Telemetry t = new Telemetry(deviceId(), ts, seq, propsMap, raw.protocol(), raw.seq() == null);
            return applyTelemetry(t);
        });
    }

    private IngestResult applyTelemetry(Telemetry t) {
        // 旧数据保护：序号更小，或序号相同但时间更旧，绝不覆盖更新鲜的状态
        boolean staleBySeq = anyTelemetry && t.seq() < reportedSeq;
        boolean staleByTs = anyTelemetry && t.seq() == reportedSeq && t.tsEpochMillis() < reportedTs;
        if (staleBySeq || staleByTs) {
            return new IngestResult(false, IngestResult.STALE, reportedSeq, reportedTs);
        }
        reported.putAll(t.properties());
        reportedTs = t.tsEpochMillis();
        reportedSeq = t.seq();
        anyTelemetry = true;
        recomputeSync();
        return new IngestResult(true, IngestResult.ACCEPTED, reportedSeq, reportedTs);
    }

    // ---------------- 命令 ----------------

    public Command enqueueCommand(String commandId, String name, Map<String, Object> params,
                                  Map<String, Object> desiredPatch) {
        return call("enqueue-command", () -> {
            if (desiredPatch != null && !desiredPatch.isEmpty()) {
                desired.putAll(desiredPatch);
                desiredVersion++;
            }
            Instant now = Instant.ofEpochMilli(clock.millis());
            Command cmd = new Command(commandId, deviceId(), name,
                    params == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(params)),
                    CommandStatus.QUEUED, 0, props.getCommandMaxAttempts(), now, null, null, null, null);
            commands.put(commandId, cmd);
            pendingQueue.addLast(commandId);
            recomputeSync();
            if (connectionState == ConnectionState.ONLINE) {
                drainQueuedCommands();
            }
            return commands.get(commandId);
        });
    }

    public Command ackCommand(String commandId, Map<String, Object> result, Map<String, Object> reportedPatch) {
        return call("ack", () -> {
            Command cmd = commands.get(commandId);
            if (cmd == null) {
                throw new GatewayException(RejectReason.UNKNOWN_COMMAND);
            }
            if (cmd.status() != CommandStatus.IN_FLIGHT) {
                return cmd; // 重复 ack 幂等
            }
            commands.put(commandId, cmd.acked(result == null ? Map.of()
                    : Map.copyOf(new LinkedHashMap<>(result)), Instant.ofEpochMilli(clock.millis())));
            if (reportedPatch != null) {
                reported.putAll(reportedPatch);
            }
            recomputeSync();
            return commands.get(commandId);
        });
    }

    /** 由网关统一的超时扫描线程周期性调用。 */
    public void sweepTimeouts() {
        run("sweep", () -> {
            long deadline = clock.millis() - props.getCommandAckTimeoutMs();
            boolean needRedispatch = false;
            for (Map.Entry<String, Command> e : commands.entrySet()) {
                Command cmd = e.getValue();
                if (cmd.status() != CommandStatus.IN_FLIGHT || cmd.lastDispatchedAt() == null
                        || cmd.lastDispatchedAt().toEpochMilli() > deadline) {
                    continue;
                }
                Command next;
                if (cmd.attempts() >= cmd.maxAttempts()) {
                    next = new Command(cmd.commandId(), cmd.deviceId(), cmd.name(), cmd.params(),
                            CommandStatus.FAILED, cmd.attempts(), cmd.maxAttempts(), cmd.createdAt(),
                            cmd.lastDispatchedAt(), cmd.ackedAt(), cmd.ackResult(),
                            "ack timeout, retries exhausted");
                } else {
                    next = new Command(cmd.commandId(), cmd.deviceId(), cmd.name(), cmd.params(),
                            CommandStatus.TIMEOUT, cmd.attempts(), cmd.maxAttempts(), cmd.createdAt(),
                            cmd.lastDispatchedAt(), cmd.ackedAt(), cmd.ackResult(),
                            "ack timeout after attempt " + cmd.attempts());
                    pendingQueue.addFirst(next.commandId()); // 超时重试优先补齐，其余命令顺序不变
                    needRedispatch = true;
                }
                commands.put(e.getKey(), next);
            }
            recomputeSync();
            if (needRedispatch && connectionState == ConnectionState.ONLINE) {
                drainQueuedCommands();
            }
        });
    }

    private void drainQueuedCommands() {
        while (session != null && !pendingQueue.isEmpty()) {
            String id = pendingQueue.peekFirst();
            Command cmd = commands.get(id);
            if (cmd == null || (cmd.status() != CommandStatus.QUEUED && cmd.status() != CommandStatus.TIMEOUT)) {
                pendingQueue.pollFirst();
                continue;
            }
            try {
                session.deliver(cmd);
            } catch (DeviceSession.TransportException ex) {
                break; // 传输暂时不可用：保持 QUEUED 与队列顺序，等待下次连接/扫描
            }
            Instant now = Instant.ofEpochMilli(clock.millis());
            commands.put(id, cmd.withStatus(CommandStatus.IN_FLIGHT, now, null));
            pendingQueue.pollFirst();
        }
    }

    // ---------------- 影子 ----------------

    public DeviceShadow updateDesired(Map<String, Object> patch) {
        return call("update-desired", () -> {
            desired.putAll(patch);
            desiredVersion++;
            recomputeSync();
            return snapshot();
        });
    }

    public DeviceShadow shadow() {
        return call("snapshot", this::snapshot);
    }

    public List<Command> commandHistory() {
        return call("history", () -> List.copyOf(commands.values()));
    }

    public Optional<Command> command(String id) {
        return call("command-by-id", () -> Optional.ofNullable(commands.get(id)));
    }

    private void recomputeSync() {
        Map<String, String> delta = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : desired.entrySet()) {
            Object actual = reported.get(e.getKey());
            if (!Objects.equals(e.getValue(), actual)) {
                delta.put(e.getKey(), "desired=" + e.getValue() + ",reported=" + actual);
            }
        }
        boolean pending = commands.values().stream().anyMatch(c -> c.status() == CommandStatus.QUEUED
                || c.status() == CommandStatus.IN_FLIGHT || c.status() == CommandStatus.TIMEOUT);
        currentDelta = delta;
        currentPending = pending;
    }

    private Map<String, String> currentDelta = Map.of();
    private boolean currentPending;

    private DeviceShadow snapshot() {
        SyncState state = !currentDelta.isEmpty() ? SyncState.OUT_OF_SYNC
                : currentPending ? SyncState.SYNCING : SyncState.IN_SYNC;
        return new DeviceShadow(deviceId(),
                Map.copyOf(desired), Map.copyOf(reported), desiredVersion,
                reportedTs, reportedSeq, state, Map.copyOf(currentDelta), connectionState);
    }
}
