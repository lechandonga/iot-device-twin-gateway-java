package com.github.lechandonga.iotdevicetwingatewayjava.model;

import java.util.Map;

/**
 * 设备影子。
 *
 * @param desired      期望值：北向写入（命令/直接设定期望）
 * @param reported     实际值：设备遥测/命令 ack 上报
 * @param desiredVersion 期望版本，每次期望写入单调递增
 * @param reportedTs     实际值最新遥测时间（乱序/迟到数据不会覆盖）
 * @param reportedSeq    实际值最新序号
 * @param syncState      IN_SYNC / OUT_OF_SYNC / SYNCING，语义见 {@link SyncState}
 */
public record DeviceShadow(String deviceId,
                           Map<String, Object> desired,
                           Map<String, Object> reported,
                           long desiredVersion,
                           long reportedTs,
                           long reportedSeq,
                           SyncState syncState,
                           Map<String, String> delta,
                           ConnectionState connectionState) {
}
