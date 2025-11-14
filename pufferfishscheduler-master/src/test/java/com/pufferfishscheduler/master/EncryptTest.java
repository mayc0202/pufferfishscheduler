package com.pufferfishscheduler.master;

import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.RSAUtil;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.Map;

/**
 *
 * @author Mayc
 * @since 2025-09-23  01:16
 */
@SpringBootTest
public class EncryptTest {

    @Resource
    private AESUtil aesUtil;

    @Test
    public void EncryptTest(){
//        String encrypt = aesUtil.encrypt("1q2w3e4R");
//        String encrypt = aesUtil.encrypt("Etl2025");
//        String encrypt = aesUtil.encrypt("123456");
//        String encrypt = aesUtil.encrypt("Ftp230515@");
        String encrypt = aesUtil.encrypt("Etledge2025@");
        System.out.println(encrypt);

        String decrypt = aesUtil.decrypt(encrypt);
        System.out.println(decrypt);
    }

    @Test
    public void EncryptTest2(){
        Map<String, String> keyMap = RSAUtil.generateKeyPair();
        String publicKey = keyMap.get("publicKey");
        String privateKey = keyMap.get("privateKey");
        System.out.println("公钥:" + publicKey);
        System.out.println("私钥:" + privateKey);
    }
}
