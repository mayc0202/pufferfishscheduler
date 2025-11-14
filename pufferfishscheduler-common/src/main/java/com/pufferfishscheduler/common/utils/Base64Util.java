package com.pufferfishscheduler.common.utils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Base64工具类
 * @author Mayc
 * @since 2025-11-13  18:10
 */
public class Base64Util {
    /**
     * 私有构造方法，防止实例化
     */
    private Base64Util() {
        throw new IllegalStateException("工具类不允许实例化");
    }

    /**
     * 对字符串进行Base64编码
     *
     * @param text 原始字符串
     * @return Base64编码后的字符串
     */
    public static String encode(String text) {
        if (text == null) {
            return null;
        }
        try {
            return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("Base64编码失败", e);
        }
    }

    /**
     * 对Base64字符串进行解码
     *
     * @param base64Text Base64编码的字符串
     * @return 解码后的原始字符串
     */
    public static String decode(String base64Text) {
        if (base64Text == null) {
            return null;
        }
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(base64Text);
            return new String(decodedBytes, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("无效的Base64字符串", e);
        } catch (Exception e) {
            throw new RuntimeException("Base64解码失败", e);
        }
    }

    /**
     * 对字节数组进行Base64编码
     *
     * @param bytes 原始字节数组
     * @return Base64编码后的字符串
     */
    public static String encode(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return Base64.getEncoder().encodeToString(bytes);
        } catch (Exception e) {
            throw new RuntimeException("Base64编码失败", e);
        }
    }

    /**
     * 对Base64字符串进行解码为字节数组
     *
     * @param base64Text Base64编码的字符串
     * @return 解码后的字节数组
     */
    public static byte[] decodeToBytes(String base64Text) {
        if (base64Text == null) {
            return null;
        }
        try {
            return Base64.getDecoder().decode(base64Text);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("无效的Base64字符串", e);
        } catch (Exception e) {
            throw new RuntimeException("Base64解码失败", e);
        }
    }

    /**
     * URL安全的Base64编码（替换'+'为'-'，'/'为'_'，去除填充'='）
     *
     * @param text 原始字符串
     * @return URL安全的Base64编码字符串
     */
    public static String encodeUrlSafe(String text) {
        if (text == null) {
            return null;
        }
        try {
            return Base64.getUrlEncoder().withoutPadding().encodeToString(
                    text.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("URL安全的Base64编码失败", e);
        }
    }

    /**
     * URL安全的Base64解码
     *
     * @param base64Text URL安全的Base64编码字符串
     * @return 解码后的原始字符串
     */
    public static String decodeUrlSafe(String base64Text) {
        if (base64Text == null) {
            return null;
        }
        try {
            byte[] decodedBytes = Base64.getUrlDecoder().decode(base64Text);
            return new String(decodedBytes, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("无效的URL安全Base64字符串", e);
        } catch (Exception e) {
            throw new RuntimeException("URL安全的Base64解码失败", e);
        }
    }

    /**
     * 检查字符串是否为有效的Base64格式
     *
     * @param text 待检查的字符串
     * @return 是否为有效的Base64格式
     */
    public static boolean isValidBase64(String text) {
        if (text == null || text.trim().isEmpty()) {
            return false;
        }
        try {
            Base64.getDecoder().decode(text);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
