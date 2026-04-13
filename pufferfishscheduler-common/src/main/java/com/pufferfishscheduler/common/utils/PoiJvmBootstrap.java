package com.pufferfishscheduler.common.utils;

import org.apache.poi.util.IOUtils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Apache POI 5 对 OOXML（ZIP）内单条条目读入有默认上限（约 100MB），大表、超长共享字符串等会触发
 * {@code RecordFormatException: maximum length for this record type is 100,000,000}。
 * 调度/ETL 场景在来源可信时放宽该限制（进程内幂等）。
 */
public final class PoiJvmBootstrap {

    /** 约 1GB，满足大 xlsx 内嵌 XML/共享串等单段解压后的读入上限 */
    public static final int DEFAULT_BYTE_ARRAY_MAX_OVERRIDE = 1_000_000_000;

    private static final AtomicBoolean APPLIED = new AtomicBoolean(false);

    private PoiJvmBootstrap() {
    }

    public static void ensureLargeOoxmlReadLimits() {
        if (!APPLIED.compareAndSet(false, true)) {
            return;
        }
        IOUtils.setByteArrayMaxOverride(DEFAULT_BYTE_ARRAY_MAX_OVERRIDE);
    }
}
