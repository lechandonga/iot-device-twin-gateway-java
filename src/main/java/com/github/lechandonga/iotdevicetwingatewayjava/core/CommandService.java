package com.github.lechandonga.iotdevicetwingatewayjava.core;

import com.github.lechandonga.iotdevicetwingatewayjava.model.Command;
import com.github.lechandonga.iotdevicetwingatewayjava.model.CommandStatus;
import com.github.lechandonga.iotdevicetwingatewayjava.model.ErrorCode;
import com.github.lechandonga.iotdevicetwingatewayjava.model.RejectException;
import com.github.lechandonga.iotdevicetwingatewayjava.registry.ConnectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 命令下发服务，管理完整生命周期。
 * 关键保证：
 * - 离线设备的命令进入有界 FIFO 队列，绝不静默丢弃；队列满时 COMMAND_QUEUE_FULL
 * - 重连后按原顺序补齐（队首投递 + 顺序保持）
 * - 投递后超时未 ACK 会回到队首重试；超过最大次数标记 EXPIRED
 * - 所有状态迁移均可通过查询接口追溯
 */
@Service
public class CommandService {

    private static final Logger log = LoggerFactory.getLogger(CommandService.class);

    private final Map<String, Command> commands = new ConcurrentHashMap<>();
    /** 每设备待投递命令（QUEUED / TIMED_OUT），按入队顺序排列，队首最先投递。 */
    private final Map<String, LinkedHashMap<String, Command>> pendingQueues = new ConcurrentHashMap<>();
    /** 正在等待 ACK 的命令。 */
    private final Map<String, Command> inFlight = new ConcurrentHashMap<>();

    private final Map<String, CommandPublisher> publishers = new ConcurrentHashMap<>();
    private final List<String> eventLog = new ArrayList<>();

    private final AtomicInteger idSequence = new AtomicInteger();
    private final GatewayProperties properties;
    private final DeviceLockManager lockManager;
    private final ConnectionRegistry connectionRegistry;
    private final IngestGate ingestGate;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "command-ack-timeout");
        thread.setDaemon(true);
        return thread;
    });

    public CommandService(GatewayProperties properties, DeviceLockManager lockManager,
                          ConnectionRegistry connectionRegistry) {
        this.properties = properties;
        this.lockManager = lockManager;
        this.connectionRegistry = connectionRegistry;
        this.ingestGate = new IngestGate(properties.getCapacity());
    }

    public void registerPublisher(CommandPublisher publisher) {
        publishers.put(publisher.transport(), publisher);
    }

    public Command issue(String deviceId, String name, Map<String, Object> payload, String preferredTransport) {
        try (AutoCloseable ignored = ingestGate.acquire()) {
            String commandId = "cmd-" + idSequence.incrementAndGet();
            Command command = new Command(commandId, deviceId, name,
                    Map.copyOf(payload), System.currentTimeMillis(), 0, 0,
                    CommandStatus.QUEUED, 0, preferredTransport, null);
            commands.put(commandId, command);

            var lock = lockManager.lockFor(deviceId);
            lock.lock();
            try {
                LinkedHashMap<String, Command> queue =
                        pendingQueues.computeIfAbsent(deviceId, key -> new LinkedHashMap<>());
                long queuedCount = queue.size() + inFlight.values().stream()
                        .filter(c -> c.deviceId().equals(deviceId)).count();
                if (queuedCount >= properties.getCommandQueueCapacity()) {
                    commands.remove(commandId);
                    throw new RejectException(ErrorCode.COMMAND_QUEUE_FULL,
                            "设备 " + deviceId + " 命令队列已满（上限 " + properties.getCommandQueueCapacity() + "）");
                }
                queue.put(commandId, command);
                recordEvent(deviceId, commandId, CommandStatus.QUEUED,
                        "命令已创建并排队（设备" + (connectionRegistry.isOnline(deviceId) ? "在线" : "离线") + "）");
                log.info("[命令] 设备 {} 命令 {} ({}) 已排队", deviceId, commandId, name);
            } finally {
                lock.unlock();
            }
            pump(deviceId, preferredTransport);
            return command;
        } catch (RejectException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 设备（重新）连接时调用：按原顺序补齐队列中的命令。
     */
    public void onDeviceConnected(String deviceId, String transport) {
        pump(deviceId, transport);
    }

    /**
     * 从队首开始投递；一条命令处于等待 ACK 时，后续命令在其后排队，保持原顺序。
     */
    private void pump(String deviceId, String transport) {
        CommandPublisher publisher = publishers.get(transport);
        if (publisher == null) {
            return;
        }
        var lock = lockManager.lockFor(deviceId);
        lock.lock();
        try {
            LinkedHashMap<String, Command> queue = pendingQueues.get(deviceId);
            while (queue != null && !queue.isEmpty()) {
                Map.Entry<String, Command> head = queue.entrySet().iterator().next();
                Command command = head.getValue();
                boolean delivered;
                try {
                    delivered = publisher.publish(command);
                } catch (Exception e) {
                    delivered = false;
                    log.warn("[命令] 设备 {} 命令 {} 传输层投递异常: {}",
                            deviceId, command.commandId(), e.getMessage());
                }
                if (!delivered) {
                    return;
                }
                queue.remove(head.getKey());
                int attempts = command.attempts() + 1;
                command = new Command(command.commandId(), deviceId, command.name(), command.payload(),
                        command.createdAt(), System.currentTimeMillis(), 0, CommandStatus.DELIVERED,
                        attempts, transport, null);
                commands.put(command.commandId(), command);
                inFlight.put(command.commandId(), command);
                recordEvent(deviceId, command.commandId(), CommandStatus.DELIVERED,
                        "第 " + attempts + " 次投递，等待 ACK");
                log.info("[命令] 设备 {} 命令 {} 已投递（第 {} 次），等待确认",
                        deviceId, command.commandId(), attempts);
                scheduleAckTimeout(command, transport);
                return;
            }
        } finally {
            lock.unlock();
        }
    }

    private void scheduleAckTimeout(Command delivered, String transport) {
        String commandId = delivered.commandId();
        String deviceId = delivered.deviceId();
        scheduler.schedule(() -> {
            var lock = lockManager.lockFor(deviceId);
            lock.lock();
            try {
                Command current = inFlight.get(commandId);
                if (current == null) {
                    return;
                }
                inFlight.remove(commandId);
                if (current.attempts() >= properties.getCommandMaxAttempts()) {
                    Command expired = current.withStatus(CommandStatus.EXPIRED,
                            current.deliveredAt(), 0, current.attempts(),
                            "已达最大投递次数 " + properties.getCommandMaxAttempts());
                    commands.put(commandId, expired);
                    recordEvent(deviceId, commandId, CommandStatus.EXPIRED,
                            "超时 " + current.attempts() + " 次仍未确认，命令终止");
                    log.warn("[命令] 设备 {} 命令 {} 超时 {} 次，标记 EXPIRED",
                            deviceId, commandId, current.attempts());
                } else {
                    Command timedOut = current.withStatus(CommandStatus.TIMED_OUT,
                            current.deliveredAt(), 0, current.attempts(),
                            "等待 ACK 超时，将回到队首重试");
                    commands.put(commandId, timedOut);
                    LinkedHashMap<String, Command> queue =
                            pendingQueues.computeIfAbsent(deviceId, key -> new LinkedHashMap<>());
                    LinkedHashMap<String, Command> reordered = new LinkedHashMap<>();
                    reordered.put(commandId, timedOut);
                    reordered.putAll(queue);
                    pendingQueues.put(deviceId, reordered);
                    recordEvent(deviceId, commandId, CommandStatus.TIMED_OUT,
                            "ACK 超时，已置于队首等待重试");
                    log.warn("[命令] 设备 {} 命令 {} ACK 超时，回到队首重试", deviceId, commandId);
                }
            } finally {
                lock.unlock();
            }
            pump(deviceId, transport);
        }, properties.getCommandAckTimeoutMs(), java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * 设备确认命令。
     */
    public Command acknowledge(String commandId, String result) {
        Command delivered = inFlight.get(commandId);
        if (delivered == null) {
            throw new RejectException(ErrorCode.COMMAND_NOT_FOUND,
                    "命令 " + commandId + " 不在等待确认状态（可能已确认/已过期/不存在）");
        }
        var lock = lockManager.lockFor(delivered.deviceId());
        lock.lock();
        try {
            Command current = inFlight.remove(commandId);
            if (current == null) {
                throw new RejectException(ErrorCode.COMMAND_NOT_FOUND, "命令 " + commandId + " 已不在等待确认状态");
            }
            Command acked = current.withStatus(CommandStatus.ACKED,
                    current.deliveredAt(), System.currentTimeMillis(), current.attempts(),
                    result == null ? "OK" : result);
            commands.put(commandId, acked);
            recordEvent(current.deviceId(), commandId, CommandStatus.ACKED,
                    "设备已确认，结果=" + acked.result());
            log.info("[命令] 设备 {} 命令 {} 已确认，结果={}", current.deviceId(), commandId, acked.result());
            pump(current.deviceId(), current.transport());
            return acked;
        } finally {
            lock.unlock();
        }
    }

    public Command getCommand(String commandId) {
        Command command = commands.get(commandId);
        if (command == null) {
            throw new RejectException(ErrorCode.COMMAND_NOT_FOUND, "命令 " + commandId + " 不存在");
        }
        return command;
    }

    public List<Command> listCommands(String deviceId) {
        return commands.values().stream()
                .filter(command -> command.deviceId().equals(deviceId))
                .sorted(java.util.Comparator.comparingLong(Command::createdAt))
                .toList();
    }

    public synchronized List<String> eventLog() {
        return List.copyOf(eventLog);
    }

    private synchronized void recordEvent(String deviceId, String commandId, CommandStatus status, String detail) {
        eventLog.add(String.format("%tT 设备=%s 命令=%s 状态=%s (%s)",
                new java.util.Date(), deviceId, commandId, status, detail));
    }

    /**
     * HTTP 长轮询：挂起中的拉取回调到达时尝试投递队首命令。
     */
    public void notifyHttpPoll(String deviceId) {
        pump(deviceId, "HTTP");
    }

    public void clear() {
        commands.clear();
        pendingQueues.clear();
        inFlight.clear();
        eventLog.clear();
    }
}
