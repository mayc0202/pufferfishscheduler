package com.pufferfishscheduler.common.utils;

import java.util.UUID;

/**
 * 通用工具类
 *
 * @author Mayc
 * @since 2026-03-17  21:16
 */
public class CommonUtil {

    /**
     * 生成UUID字符串
     *
     * @return UUID字符串
     */
    public static String getUUIDString() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
