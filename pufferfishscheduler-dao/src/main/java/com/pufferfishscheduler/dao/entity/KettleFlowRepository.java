/*
 * Inc.
 * Copyright (c) 2024 All Rights Reserved.
 */
package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

/**
 * Kettle流程仓库表 Entity
 * 存储转换和作业的配置信息
 *
 * @author Mayc
 * @date 2026-03-07 16:37:26
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@TableName(value = "kettle_flow_repository")
public class KettleFlowRepository implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 主键ID，UUID
     */
    @TableId(type = IdType.ASSIGN_UUID)
    private String id;

    /**
     * 业务类型
     */
    @TableField("biz_type")
    private String bizType;

    /**
     * 业务表关联ID
     */
    @TableField("biz_object_id")
    private String bizObjectId;

    /**
     * 流程类型：TRANS-转换，JOB-作业
     */
    @TableField("flow_type")
    private String flowType;

    /**
     * Kettle流程XML配置内容
     */
    @TableField("flow_content")
    private String flowContent;

    /**
     * 前端设计器JSON配置（冗余字段，用于快速加载）
     */
    @TableField("flow_json")
    private String flowJson;

    /**
     * 执行器主机地址，用于分布式调度
     */
    @TableField("executor_host")
    private String executorHost;

    /**
     * 执行器端口
     */
    @TableField("executor_port")
    private Integer executorPort;

    /**
     * 版本号，用于乐观锁控制
     */
    @TableField("version")
    private Integer version;

    /**
     * 流程状态：0-失效，1-生效
     */
    @TableField("flow_status")
    private Integer flowStatus;

    /**
     * 创建人
     */
    @TableField("created_by")
    private String createdBy;

    /**
     * 创建时间
     */
    @TableField("created_time")
    private Date createdTime;

    /**
     * 更新人
     */
    @TableField("updated_by")
    private String updatedBy;

    /**
     * 更新时间
     */
    @TableField("updated_time")
    private Date updatedTime;

}