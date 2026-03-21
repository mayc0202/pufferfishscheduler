package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
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
 * 数据转换任务表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("trans_task")
public class TransTask implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    @TableField(value = "name")
    private String name;

    @TableField(value = "remark")
    private String remark;

    @TableField(value = "flow_id")
    private Integer flowId;

    @TableField(value = "execute_type")
    private String executeType;

    @TableField(value = "cron")
    private String cron;

    @TableField(value = "failure_policy")
    private String failurePolicy;

    @TableField(value = "notify_policy")
    private String notifyPolicy;

    @TableField(value = "status")
    private String status;

    @TableField(value = "enable")
    private Boolean enable;

    @TableField(value = "deleted")
    private Boolean deleted;

    @TableField(value = "reason")
    private String reason;

    @TableField(value = "execute_time")
    private Date executeTime;

    @TableField(value = "created_by", fill = FieldFill.INSERT)
    private String createdBy;

    @TableField(value = "created_time", fill = FieldFill.INSERT)
    private Date createdTime;

    @TableField(value = "updated_by", fill = FieldFill.INSERT_UPDATE)
    private String updatedBy;

    @TableField(value = "updated_time", fill = FieldFill.INSERT_UPDATE)
    private Date updatedTime;
}

