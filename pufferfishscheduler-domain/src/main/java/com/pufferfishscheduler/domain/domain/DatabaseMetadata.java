package com.pufferfishscheduler.domain.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 数据源元数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DatabaseMetadata {
    /**
     * 自增主键
     */
    private Integer id;

    /**
     * 数据库接入名称
     */
    private String name;

    /**
     * 数据库分类：R-关系型数据库；NR-非关系数据库；
     */
    private String category;

    /**
     * 数据库类型：Mysql、Oracle、SQLServer、PostgreSQL、GaussDB、Hive、Spark、Greenplum等
     */
    private String type;

    /**
     * 主机地址
     */
    private String host;

    /**
     * 端口
     */
    private String port;

    /**
     * 数据库名称/SID
     */
    private String dbName;

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
     * 备注
     */
    private String remark;

    private String properties;

    private String extConfig;

    private List<String> allowedTables;
}
