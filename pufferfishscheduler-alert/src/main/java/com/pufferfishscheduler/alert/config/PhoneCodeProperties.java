package com.pufferfishscheduler.alert.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 手机号验证码配置类
 *
 * @author Mayc
 * @since 2026-03-23  13:44
 */
@Data
@Component
@ConfigurationProperties(prefix = "phoneconfig")
public class PhoneCodeProperties {
    private String accessKey; // 密钥id
    private String secret; // 密钥
    private String signName; //
    private String templateCode; // 短信模板
    private String apiversion; // api版本号
    private String domain; // 终端
    private String action;
}
