package com.pufferfishscheduler.dao.entity;


import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * (DbDatabase)表实体类
 *
 * @author mayc
 * @since 2025-06-03 21:22:27
 */
@Data
@TableName(value = "db_database")
public class DbDatabase implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId
    private Integer id;

    // 数据源名称
    @TableField(value = "`name`")
    private String name;

    @TableField(value = "group_id")
    private Integer groupId;

    /**
     * 数据源分层 1-业务库BIZ；2-贴源库ODS；3-治理库DW；4-应用库ADS；5-共享库DS
     */
    @TableField(value = "label")
    private String label;

    /**
     * 数据库大类：1-关系型数据库；2-非关系型数据库；3-消息型数据库；4-FTP类型; 5-OSS
     */
    @TableField(value = "category")
    private String category;

    /**
     * 数据库类型：MySQL、Oracle、SQLServer、PostgresSQL等
     */
    @TableField(value = "type")
    private String type;

    /**
     * 主机地址
     */
    @TableField(value = "db_host")
    private String dbHost;

    /**
     * 端口
     */
    @TableField(value = "db_port")
    private String dbPort;

    /**
     * 数据库名称/SID
     */
    @TableField(value = "db_name")
    private String dbName;

    /**
     * 模式schema
     */
    @TableField(value = "db_schema")
    private String dbSchema;

    /**
     * 数据库用户名
     */
    @TableField(value = "username")
    private String username;

    /**
     * 数据库密码
     */
    @TableField(value = "password")
    private String password;

    /**
     * 备注
     */
    @TableField(value = "remark")
    private String remark;

    /**
     * 是否按天做增量统计。默认值：不需要增量统计
     */
    @TableField(value = "is_stats_by_day")
    private Integer isStatsByDay;

    /**
     * 最近同步时间
     */
    @TableField(value = "last_sync_time")
    private Date lastSyncTime;

    /**
     * 数据同步周期
     */
    @TableField(value = "data_sync_cycle")
    private Integer dataSyncCycle;

    /**
     * 是否解除：0-未解除；1-已解除，默认未删除
     */
    @TableField(value = "deleted")
    private Boolean deleted;

    /**
     * 创建人
     */
    @TableField(value = "created_by")
    private String createdBy;

    /**
     * 创建时间
     */
    @TableField(value = "created_time")
    private Date createdTime;

    /**
     * 更新人
     */
    @TableField(value = "updated_by")
    private String updatedBy;

    /**
     * 更新时间
     */
    @TableField(value = "updated_time")
    private Date updatedTime;

    /**
     * 属性
     */
    @TableField(value = "properties")
    private String properties;

    /**
     * 扩展配置
     */
    @TableField(value = "ext_config")
    private String extConfig;

}

