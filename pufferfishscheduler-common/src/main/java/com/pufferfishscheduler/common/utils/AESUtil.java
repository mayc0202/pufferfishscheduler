package com.pufferfishscheduler.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * AES加密解密工具类（Spring管理的单例Bean）
 * 采用AES算法，ECB模式，PKCS5Padding填充方式
 * 密钥从外部配置注入，线程安全
 *
 * @author mayc
 * @since 2025-09-21
 */
@Slf4j
@Component
public class AESUtil {

    // 算法常量（不变）
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/ECB/PKCS5Padding";
    private static final int KEY_LENGTH_BYTES = 16; // AES-128固定16字节密钥

    // 实例变量存储密钥（Spring注入后初始化，线程安全）
    private final SecretKeySpec secretKey;

    /**
     * 构造方法注入密钥并初始化SecretKey（替代@PostConstruct）
     * 直接在构造阶段完成密钥验证和处理，避免二次赋值
     */
    public AESUtil(@Value("${security.aes.key}") String aesKey) {
        // 验证密钥配置
        if (aesKey == null || aesKey.trim().isEmpty()) {
            log.error("AES密钥配置不能为空！请检查application.yml中的security.aes.key");
            throw new IllegalArgumentException("AES密钥配置缺失");
        }
        // 处理密钥并初始化（只执行一次，提升性能）
        this.secretKey = createSecretKey(aesKey);
        log.info("AESUtil初始化完成，密钥已加载");
    }

    /**
     * 创建SecretKeySpec（封装密钥处理逻辑）
     */
    private SecretKeySpec createSecretKey(String key) {
        try {
            // SHA-256哈希确保密钥强度，取前16字节作为AES-128密钥
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] keyBytes = digest.digest(key.getBytes(StandardCharsets.UTF_8));
            byte[] processedKey = Arrays.copyOf(keyBytes, KEY_LENGTH_BYTES);
            return new SecretKeySpec(processedKey, ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            log.error("SHA-256算法不支持", e);
            throw new SecurityException("密钥处理失败", e);
        }
    }

    /**
     * 使用默认密钥加密（实例方法，依赖Spring注入的密钥）
     */
    public String encrypt(String content) {
        if (content == null) {
            log.error("加密内容不能为空");
            throw new IllegalArgumentException("加密内容不能为空");
        }

        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            byte[] encrypted = cipher.doFinal(content.getBytes(StandardCharsets.UTF_8));
            return Base64.encodeBase64String(encrypted);
        } catch (Exception e) {
            log.error("AES加密失败，内容：{}", content, e);
            throw new SecurityException("AES加密失败", e);
        }
    }

    /**
     * 使用默认密钥解密（实例方法，依赖Spring注入的密钥）
     */
    public String decrypt(String encryptedContent) {
        if (encryptedContent == null) {
            log.error("解密内容不能为空");
            throw new IllegalArgumentException("解密内容不能为空");
        }

        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            byte[] encryptedBytes = Base64.decodeBase64(encryptedContent.trim());
            byte[] original = cipher.doFinal(encryptedBytes);
            return new String(original, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("AES解密失败，内容：{}", encryptedContent, e);
            throw new SecurityException("AES解密失败", e);
        }
    }

    /**
     * 提供指定密钥的加密方法（静态方法，供灵活场景使用）
     */
    public static String encryptWithKey(String content, String key) {
        if (content == null || key == null) {
            throw new IllegalArgumentException("加密内容或密钥不能为空");
        }
        try {
            SecretKeySpec tempKey = createTempSecretKey(key);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, tempKey);
            byte[] encrypted = cipher.doFinal(content.getBytes(StandardCharsets.UTF_8));
            return Base64.encodeBase64String(encrypted);
        } catch (Exception e) {
            log.error("AES指定密钥加密失败", e);
            throw new SecurityException("AES加密失败", e);
        }
    }

    /**
     * 提供指定密钥的解密方法（静态方法，供灵活场景使用）
     */
    public static String decryptWithKey(String encryptedContent, String key) {
        if (encryptedContent == null || key == null) {
            throw new IllegalArgumentException("解密内容或密钥不能为空");
        }
        try {
            SecretKeySpec tempKey = createTempSecretKey(key);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, tempKey);
            byte[] encryptedBytes = Base64.decodeBase64(encryptedContent.trim());
            byte[] original = cipher.doFinal(encryptedBytes);
            return new String(original, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("AES指定密钥解密失败", e);
            throw new SecurityException("AES解密失败", e);
        }
    }

    /**
     * 创建临时密钥（供静态方法使用）
     */
    private static SecretKeySpec createTempSecretKey(String key) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] keyBytes = digest.digest(key.getBytes(StandardCharsets.UTF_8));
        byte[] processedKey = Arrays.copyOf(keyBytes, KEY_LENGTH_BYTES);
        return new SecretKeySpec(processedKey, ALGORITHM);
    }
}
