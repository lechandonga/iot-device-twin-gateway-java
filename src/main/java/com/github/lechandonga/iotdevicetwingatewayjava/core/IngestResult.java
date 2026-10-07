package com.github.lechandonga.iotdevicetwingatewayjava.core;

/**
 * 遥测处理结果。
 *
 * @param accepted false 表示因乱序/迟到被保护性忽略（不是鉴权类拒绝）
 * @param reason   STALE_TELEMETRY_IGNORED 或 ACCEPTED
 */
public record IngestResult(boolean accepted, String reason, long shadowReportedSeq, long shadowReportedTs) {
    public static final String ACCEPTED = "ACCEPTED";
    public static final String STALE = "STALE_TELEMETRY_IGNORED";
}
