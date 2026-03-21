package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * rt_task
 * 实时同步任务表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "rt_task")
public class RtTask implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 任务ID
     */
    @TableId
    private Integer id;

    /**
     * 任务名称
     */
    @TableField(value = "name")
    private String name;

    /**
     * 基础配置模块业务主题表id
     */
    @TableField(value = "group_id")
    private String groupId;

    /**
     * 源数据库ID。同一个源数据库和目标数据库只能新增一个任务
     */
    @TableField(value = "source_db_id")
    private Integer sourceDbId;

    /**
     * 目标数据库ID
     */
    @TableField(value = "target_db_id")
    private Integer targetDbId;

    /**
     * 数据同步类型。0 - 全量+增量； 1 - 增量
     */
    @TableField(value = "sync_type")
    private String syncType;

    /**
     * 同步引擎类型。0 - Kafka； 1 - Flink
     */
    @TableField(value = "engine_type")
    private String engineType;

    /**
     * 任务状态。INIT - 未启动（首次添加进来时此状态）；RUNNING - 运行中；STOP - 已停止；FAILURE - 异常
     */
    @TableField(value = "status")
    private String status;

    /**
     * 异常原因
     */
    @TableField(value = "reason")
    private String reason;

    /**
     * 实时数据同步运行配置。实时任务启动后，此值从实时数据同步执行引擎返回。每次启动任务时，需要将此参数传递给执行引擎，启动成功后执行器会返回此值，需要将此值记录到表中
     */
    @TableField(value = "runtime_config")
    private String runtimeConfig;

    /**
     * 数据任务类型 1-归集；2-治理；3-开发；4-共享
     */
    @TableField(value = "stage")
    private String stage;

    /**
     * 是否开启心跳
     */
    @TableField(value = "heartbeat_enabled")
    private Boolean heartbeatEnabled;

    /**
     * 心跳频率
     */
    @TableField(value = "heartbeat_interval")
    private Integer heartbeatInterval;

    /**
     * 是否删除，0-否；1-是
     */
    @TableField(value = "deleted")
    private Boolean deleted;

    /**
     * 创建人账号
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

}
