package com.pufferfishscheduler.domain.model.database;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 数据源连接信息
 *
 * @author Mayc
 * @since 2025-09-06  23:45
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DBConnectionInfo {

    /**
     * 数据源id
     */
    private Integer id;

    /**
     * 客户端id
     */
    private String clientId;

    /**
     * 数据源名称
     */
    private String name;

    /**
     * 数据源类型
     */
    private String type;

    /**
     * 主机地址
     */
    private String dbHost;

    /**
     * 端口
     */
    private String dbPort;

    /**
     * 数据库名称
     */
    private String dbName;

    /**
     * 数据库索引
     */
    private Integer databaseIndex;

    /**
     * 模式schema
     */
    private String dbSchema;

    /**
     * 数据库用户名
     */
    private String username;

    /**
     * 数据库密码
     */
    private String password;

    /**
     * 主题
     */
    private String topic;

    /**
     * 队列
     */
    private String queue;

    /**
     * 属性
     */
    private String properties;

    /**
     * 扩展配置
     */
    private String extConfig;

    /**
     * be地址
     */
    private String beAddress;

    /**
     * fe地址
     */
    private String feAddress;

    /**
     * 连接方式
     */
    private String connectType;

    /**
     * 控制编码
     */
    private String controlEncoding;

    /**
     * FTP主动/被动模式
     */
    private String mode;

    private Boolean useSSL;

    private Boolean useCopy;

    private String authWay;

    private Integer connectionTimeout;

    private Integer readTimeout;

    private String secretId;

    private String secretKey;

    private String odpsEndpoint;

    private String tunnelEndpoint;

    private String project;

    private String accessKeyId;

    private String accessKeySecret;

    private String shareType;

    private String dbConnectType;

    private String accessKey;
}
