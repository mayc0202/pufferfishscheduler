package com.pufferfishscheduler.common.utils;

import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import java.util.Base64;

/**
 * 密钥生成器工具类
 * @author Mayc
 * @since 2026-03-01  22:03
 */
public class KeyGeneratorUtil {

    /**
     * 生成安全的HS256密钥（返回Base64编码的字符串）
     */
    public static String generateSecureKey() {
        SecretKey key = Keys.secretKeyFor(SignatureAlgorithm.HS256);
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    /**
     * 生成安全的HS384密钥
     */
    public static String generateSecureKeyForHS384() {
        SecretKey key = Keys.secretKeyFor(SignatureAlgorithm.HS384);
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    /**
     * 生成安全的HS512密钥
     */
    public static String generateSecureKeyForHS512() {
        SecretKey key = Keys.secretKeyFor(SignatureAlgorithm.HS512);
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    public static void main(String[] args) {
        System.out.println("生成的HS256密钥: " + generateSecureKey());
        System.out.println("生成的HS384密钥: " + generateSecureKeyForHS384());
        System.out.println("生成的HS512密钥: " + generateSecureKeyForHS512());
    }
}
