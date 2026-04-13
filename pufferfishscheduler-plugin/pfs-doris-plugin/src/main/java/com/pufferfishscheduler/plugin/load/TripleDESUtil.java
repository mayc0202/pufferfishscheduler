package com.pufferfishscheduler.plugin.load;

//import com.sun.org.apache.xml.internal.security.utils.Base64;
import jcifs.util.Base64;
import org.apache.commons.lang.StringUtils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class TripleDESUtil {

    //算法名称
    private static final String Algorithm = "DESede";
    // 算法名称/加密模式/填充方式
    private static final String CIPHER_ALGORITHM_ECB = "DESede/ECB/PKCS5Padding";
    //自定义24位长加密密钥
    private static final String key = "ed2a0c42518395c6af183e3a";

    /**
     * 3des解密
     * @param value 待解密字符串
     * @return
     * @throws Exception
     */
    public static String decrypt3DES(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        try {
            byte[] b = decryptMode(getKeyBytes(key), Base64.decode(value));
            return new String(b);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    /**
     * 3des加密
     * @param value 待加密字符串
     * @return
     * @throws Exception
     */
    public static String encrypt3DES(String value){
        try {
            String str = byte2Base64(encryptMode(getKeyBytes(key), value.getBytes()));
            return str;
        } catch (Exception e) {
            System.out.println("解密失败！");
        }
        return value;
    }

    /**
     * 获取key 24位长的密码byte值
     * @param strKey
     * @return
     * @throws Exception
     */
    private static byte[] getKeyBytes(String strKey) throws Exception {
        if (null == strKey || strKey.length() < 1) {
            throw new Exception("key is null or empty!");
        }
        byte[] bkey = strKey.getBytes();
        int start = bkey.length;
        byte[] bkey24 = new byte[24];
        //补足24字节
        for (int i = 0; i < start; i++) {
            bkey24[i] = bkey[i];
        }
        for (int i = start; i < 24; i++) {
            bkey24[i] = '\0';
        }
        return bkey24;
    }

    /**
     *
     * @param keybyte 为加密密钥，长度为24字节
     * @param src 为被加密的数据缓冲区（源）
     * @return
     */
    private static byte[] encryptMode(byte[] keybyte, byte[] src) throws Exception {
        SecretKey deskey = new SecretKeySpec(keybyte, Algorithm);
        Cipher c1 = Cipher.getInstance(CIPHER_ALGORITHM_ECB);
        c1.init(Cipher.ENCRYPT_MODE, deskey);
        return c1.doFinal(src);
    }

    /**
     *
     * @param keybyte 为加密密钥，长度为24字节
     * @param src 为加密后的缓冲区
     * @return
     */
    private static byte[] decryptMode(byte[] keybyte, byte[] src) throws Exception {
        SecretKey deskey = new SecretKeySpec(keybyte, Algorithm);
        Cipher c1 = Cipher.getInstance(CIPHER_ALGORITHM_ECB);
        c1.init(Cipher.DECRYPT_MODE, deskey);
        return c1.doFinal(src);
    }

    /**
     * 转换成base64编码
     * @param b
     * @return
     */
    private static String byte2Base64(byte[] b) {
        return Base64.encode(b);
    }


}
