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
 * 流程节点执行日志
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("trans_task_step_log")
@SuppressWarnings("unused")
public class TransTaskStepLog implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 批次ID，关联 trans_task_log.id
     */
    @TableField(value = "task_log_id")
    private String taskLogId;

    /**
     * 步骤名称
     */
    @TableField(value = "step_name")
    private String stepName;

    /**
     * 状态: S-成功, F-失败
     */
    @TableField(value = "status")
    private String status;

    @TableField(value = "reason")
    private String reason;

    @TableField(value = "lines_read")
    private Long linesRead;

    @TableField(value = "lines_written")
    private Long linesWritten;

    @TableField(value = "lines_updated")
    private Long linesUpdated;

    @TableField(value = "lines_input")
    private Long linesInput;

    @TableField(value = "lines_output")
    private Long linesOutput;

    @TableField(value = "lines_rejected")
    private Long linesRejected;

    @TableField(value = "lines_errors")
    private Long linesErrors;

    @TableField(value = "start_time")
    private Date startTime;

    @TableField(value = "end_time")
    private Date endTime;

    /**
     * 创建人账号
     */
    @TableField(value = "created_by", fill = FieldFill.INSERT)
    private String createdBy;

    /**
     * 创建时间
     */
    @TableField(value = "created_time", fill = FieldFill.INSERT)
    private Date createdTime;
}
