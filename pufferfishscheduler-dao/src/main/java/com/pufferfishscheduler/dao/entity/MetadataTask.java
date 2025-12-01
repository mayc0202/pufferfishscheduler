package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * 元数据同步任务
 *
 * @author Mayc
 * @since 2025-11-25  15:07
 */
@Data
@TableName("metadata_task")
public class MetadataTask implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    /**
     * 数据源id
     */
    @TableField("db_id")
    private Integer dbId;

    /**
     * 工作组id
     */
    @TableField("worker_id")
    private Integer workerId;

    /**
     * cron执行表达式
     */
    @TableField("cron")
    private String cron;

    /**
     * 计划执行错误策略（0继续 1结束）
     */
    @TableField("failure_policy")
    private String failurePolicy;

    /**
     * 通知策略（0都不发 1成功时通知 2失败时通知 3全部通知）
     */
    @TableField("notify_policy")
    private String notifyPolicy;

    /**
     * 禁用状态（0启用 1禁用）
     */
    @TableField("enable")
    private Boolean enable;

    /**
     * 状态 0-未启动 1-启动中 2-运行中 3-成功 4-失败 5-停止中 6-已停止 7-异常
     */
    @TableField("status")
    private String status;

    /**
     * 备注
     */
    @TableField("remark")
    private String remark;

    /**
     * 执行时间
     */
    @TableField("execute_time")
    private Date executeTime;

    /**
     * 是否解除：0-未解除；1-已解除，默认未删除
     */
    @TableField(value = "deleted")
    private Boolean deleted;

    /**
     * 创建人
     */
    @TableField(value = "created_by", fill = FieldFill.INSERT)
    private String createdBy;

    /**
     * 创建时间
     */
    @TableField(value = "created_time", fill = FieldFill.INSERT)
    private Date createdTime;

    /**
     * 修改人
     */
    @TableField(value = "updated_by", fill = FieldFill.INSERT_UPDATE)
    private String updatedBy;

    /**
     * 修改时间
     */
    @TableField(value = "updated_time", fill = FieldFill.INSERT_UPDATE)
    private Date updatedTime;
}
