package com.pufferfishscheduler.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.ByteArrayOutputStream;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;

/**
 * RSA 非对称加密解密工具类（Spring管理单例Bean）
 * 核心能力：公钥加密、私钥解密、数字签名/验证（支持长文本分段处理）
 * 算法配置：RSA/ECB/PKCS1Padding（明确填充方式，避免跨环境差异）
 * 密钥规格：2048位（符合NIST安全标准，替代1024位）
 *
 * @author Mayc
 * @since 2025-09-21
 */
@Slf4j
@Component
public class RSAUtil {
    // 算法常量（不可修改，用final修饰）
    private static final String KEY_ALGORITHM = "RSA";
    private static final String CIPHER_ALGORITHM = "RSA/ECB/PKCS1Padding";
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA"; // 签名算法（SHA256比MD5更安全）
    private static final int KEY_SIZE = 2048; // 升级为2048位密钥
    private static final int MAX_ENCRYPT_BLOCK = 245; // 2048位密钥对应最大加密块（2048/8 - 11 = 245）
    private static final int MAX_DECRYPT_BLOCK = 256; // 2048位密钥对应最大解密块（2048/8 = 256）

    // 密钥对象（实例变量，Spring注入后初始化，线程安全）
    private final PublicKey publicKey;
    private final PrivateKey privateKey;

    /**
     * 构造方法注入密钥（从配置文件获取，避免硬编码）
     * 直接在构造阶段完成密钥验证和解析，确保Bean初始化时密钥可用
     *
     * @param rsaPublicKey 配置文件中的Base64编码公钥（security.rsa.public-key）
     * @param rsaPrivateKey 配置文件中的Base64编码私钥（security.rsa.private-key）
     */
    public RSAUtil(
            @Value("${security.rsa.public-key}") String rsaPublicKey,
            @Value("${security.rsa.private-key}") String rsaPrivateKey
    ) {
        // 1. 验证密钥配置（Spring Assert工具类，简化非空校验）
        Assert.hasText(rsaPublicKey, "RSA公钥配置不能为空！请检查application.yml中的security.rsa.public-key");
        Assert.hasText(rsaPrivateKey, "RSA私钥配置不能为空！请检查application.yml中的security.rsa.private-key");

        // 2. 解析密钥并初始化（异常直接抛出，确保Bean初始化失败时快速反馈）
        try {
            this.publicKey = parsePublicKey(rsaPublicKey);
            this.privateKey = parsePrivateKey(rsaPrivateKey);
            log.info("RSAUtil初始化完成，密钥规格：{}位", KEY_SIZE);
        } catch (Exception e) {
            log.error("RSA密钥解析失败，公钥：{}，私钥：{}", rsaPublicKey.substring(0, 20) + "...", rsaPrivateKey.substring(0, 20) + "...", e);
            throw new SecurityException("RSA密钥初始化失败", e);
        }
    }

    // ---------------------- 核心加密解密方法（实例方法，依赖注入的密钥） ----------------------

    /**
     * 公钥加密（支持长文本分段处理）
     * @param plainText 待加密的明文（不能为空）
     * @return Base64编码的加密结果
     */
    public String encrypt(String plainText) {
        Assert.hasText(plainText, "加密明文不能为空");
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);

            byte[] plainBytes = plainText.getBytes();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int offset = 0;
            int len = plainBytes.length;

            // 分段加密（处理超过MAX_ENCRYPT_BLOCK的长文本）
            while (offset < len) {
                int blockLen = Math.min(len - offset, MAX_ENCRYPT_BLOCK);
                byte[] encryptedBlock = cipher.doFinal(plainBytes, offset, blockLen);
                out.write(encryptedBlock);
                offset += blockLen;
            }

            return Base64.encodeBase64String(out.toByteArray());
        } catch (Exception e) {
            log.error("RSA加密失败，明文：{}（前50字符）", plainText.substring(0, Math.min(plainText.length(), 50)), e);
            throw new SecurityException("RSA加密失败", e);
        }
    }

    /**
     * 私钥解密（支持长文本分段处理）
     * @param encryptedText Base64编码的加密文本（不能为空）
     * @return 解密后的明文
     */
    /**
     * 私钥解密（支持长文本分段处理）
     */
    public String decrypt(String encryptedText) {
        Assert.hasText(encryptedText, "解密文本不能为空");
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, privateKey);

            byte[] encryptedBytes = Base64.decodeBase64(encryptedText);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int offset = 0;
            int len = encryptedBytes.length;

            // 分段解密
            while (offset < len) {
                int blockLen = Math.min(len - offset, MAX_DECRYPT_BLOCK);
                byte[] decryptedBlock = cipher.doFinal(encryptedBytes, offset, blockLen);
                out.write(decryptedBlock);
                offset += blockLen;
            }

            return new String(out.toByteArray());
        } catch (BadPaddingException e) {
            log.error("RSA解密失败：密钥不匹配或密文被篡改");
            throw new SecurityException("解密失败：请检查密钥是否匹配", e);
        } catch (Exception e) {
            log.error("RSA解密失败", e);
            throw new SecurityException("RSA解密失败", e);
        }
    }

    public static byte[] decryptByPrivateKey(byte[] data, String key) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {

        Map<String, Object> map = privateKey(key);
        Cipher cipher = (Cipher) map.get("cipher");
        Key privateKey = (Key) map.get("privateKey");

        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        return cipher.doFinal(data);
    }

    public static Map<String, Object> privateKey(String key) throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException {

        Map<String, Object> map = new HashMap<>(2);

        // 对密钥解密
        byte[] keyBytes = decryptBASE64(key);
        // 取得私钥
        PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
        Key privateKey = keyFactory.generatePrivate(pkcs8KeySpec);
        // 对数据加密
        String algorithm = keyFactory.getAlgorithm();
        Cipher cipher = Cipher.getInstance(algorithm);

        map.put("privateKey", privateKey);
        map.put("cipher", cipher);

        return map;
    }

    /**
     * 将String字符串编码为Base64数组
     * @param key String
     * @return
     */
    private static byte[] decryptBASE64(String key) {
        return Base64.decodeBase64(key);
    }


    // ---------------------- 数字签名与验证（确保数据完整性和防篡改） ----------------------

    /**
     * 私钥生成数字签名（验证数据是否被篡改）
     * @param data 待签名的数据（不能为空）
     * @return Base64编码的签名结果
     */
    public String sign(String data) {
        Assert.hasText(data, "签名数据不能为空");
        try {
            Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
            signature.initSign(privateKey);
            signature.update(data.getBytes());
            return Base64.encodeBase64String(signature.sign());
        } catch (Exception e) {
            log.error("RSA签名失败，数据：{}（前50字符）", data.substring(0, Math.min(data.length(), 50)), e);
            throw new SecurityException("RSA签名失败", e);
        }
    }

    /**
     * 公钥验证数字签名（验证数据完整性）
     * @param data 原始数据（不能为空）
     * @param sign Base64编码的签名（不能为空）
     * @return true：签名验证通过；false：签名验证失败
     */
    public boolean verifySign(String data, String sign) {
        Assert.hasText(data, "验证数据不能为空");
        Assert.hasText(sign, "签名不能为空");
        try {
            Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
            signature.initVerify(publicKey);
            signature.update(data.getBytes());
            return signature.verify(Base64.decodeBase64(sign));
        } catch (Exception e) {
            log.error("RSA签名验证失败，数据：{}（前50字符），签名：{}（前50字符）",
                    data.substring(0, Math.min(data.length(), 50)),
                    sign.substring(0, Math.min(sign.length(), 50)),
                    e);
            throw new SecurityException("RSA签名验证失败", e);
        }
    }

    // ---------------------- 工具方法（密钥解析、密钥对生成） ----------------------

    /**
     * 解析Base64编码的公钥为PublicKey对象
     * @param publicKeyStr Base64编码的公钥
     * @return PublicKey对象
     */
    private PublicKey parsePublicKey(String publicKeyStr) throws Exception {
        byte[] keyBytes = Base64.decodeBase64(publicKeyStr);
        X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
        return keyFactory.generatePublic(keySpec);
    }

    /**
     * 解析Base64编码的私钥为PrivateKey对象
     * @param privateKeyStr Base64编码的私钥
     * @return PrivateKey对象
     */
    private PrivateKey parsePrivateKey(String privateKeyStr) throws Exception {
        byte[] keyBytes = Base64.decodeBase64(privateKeyStr);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
        return keyFactory.generatePrivate(keySpec);
    }

    /**
     * 生成RSA密钥对（用于初始化配置文件，非运行时调用）
     * @return 包含公钥（Base64编码）和私钥（Base64编码）的Map
     */
    public static Map<String, String> generateKeyPair() {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
            keyPairGenerator.initialize(KEY_SIZE, new SecureRandom()); // 加随机数种子，提升密钥随机性
            KeyPair keyPair = keyPairGenerator.generateKeyPair();

            // 密钥转为Base64编码（方便配置文件存储）
            String publicKeyStr = Base64.encodeBase64String(keyPair.getPublic().getEncoded());
            String privateKeyStr = Base64.encodeBase64String(keyPair.getPrivate().getEncoded());

            Map<String, String> keyMap = new HashMap<>(2);
            keyMap.put("publicKey", publicKeyStr);
            keyMap.put("privateKey", privateKeyStr);
            log.info("RSA密钥对生成完成（{}位），公钥：{}，私钥：{}",
                    KEY_SIZE, publicKeyStr.substring(0, 30) + "...", privateKeyStr.substring(0, 30) + "...");
            return keyMap;
        } catch (NoSuchAlgorithmException e) {
            log.error("RSA密钥对生成失败", e);
            throw new SecurityException("RSA密钥对生成失败", e);
        }
    }

    // ---------------------- Getter（仅用于必要的密钥暴露，如对外提供公钥） ----------------------
    public String getPublicKeyStr() {
        return Base64.encodeBase64String(publicKey.getEncoded());
    }
}