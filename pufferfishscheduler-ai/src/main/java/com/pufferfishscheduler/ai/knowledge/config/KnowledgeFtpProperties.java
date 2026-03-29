package com.pufferfishscheduler.ai.knowledge.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 知识库附件FTP配置
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "knowledge.ftp")
public class KnowledgeFtpProperties {

    /**
     * FTP服务地址
     */
    private String host;

    /**
     * FTP服务端口
     */
    private Integer port = 21;

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * FTP根目录
     */
    private String basePath = "/knowledge";

    /**
     * 是否启用被动模式
     */
    private Boolean passiveMode = true;

    /**
     * 控制编码
     */
    private String controlEncoding = "UTF-8";
}
