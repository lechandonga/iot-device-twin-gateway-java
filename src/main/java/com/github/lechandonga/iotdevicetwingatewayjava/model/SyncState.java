package com.github.lechandonga.iotdevicetwingatewayjava.model;

/**
 * 影子同步状态：
 * OUT_OF_SYNC：存在期望/实际不一致的键（delta 非空）；
 * SYNCING：delta 已清空但仍有未完成（QUEUED/IN_FLIGHT/TIMEOUT）命令；
 * IN_SYNC：delta 为空且没有在途命令。
 */
public enum SyncState {
    IN_SYNC, SYNCING, OUT_OF_SYNC
}
