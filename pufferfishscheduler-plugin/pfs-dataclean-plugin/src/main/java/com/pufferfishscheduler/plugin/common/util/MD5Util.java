package com.pufferfishscheduler.plugin.common.util;


import com.pufferfishscheduler.plugin.common.Constants;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Util {

    /**
     * 加密
     * @param plaintext 明文
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String encrypt(String plaintext) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance(Constants.MD5);
        md.update(plaintext.getBytes());
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    /**
     * 解密
     * @param plaintext 明文
     * @param encrypted 加密后
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static boolean decrypt(String plaintext, String encrypted) throws NoSuchAlgorithmException {
        String decrypted = encrypt(plaintext);
        return decrypted.equals(encrypted);
    }

}
