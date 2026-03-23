package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.FieldStrategy;
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
 * 任务执行日志表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("trans_task_log")
@SuppressWarnings("unused")
public class TransTaskLog implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 批次ID
     */
    @TableId(value = "id")
    private String id;

    /**
     * 任务ID，关联 trans_task.id
     */
    @TableField(value = "task_id")
    private Integer taskId;

    /**
     * 执行状态：
     * INIT-未启动、RUNNING-运行中、SUCESS-成功、STOP-已停止、FAILURE-异常、STARTING-启动中、STOPPING-停止中
     */
    @TableField(value = "status")
    private String status;

    /**
     * 执行方式：1-定时执行；2-手动执行
     */
    @TableField(value = "executing_type")
    private String executingType;

    @TableField(value = "data_volume")
    private Long dataVolume;

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

    @TableField(value = "reason")
    private String reason;

    @TableField(value = "flow_image")
    private String flowImage;

    @TableField(value = "parameters")
    private String parameters;

    @TableField(value = "log_detail")
    private String logDetail;

    @TableField(value = "start_time")
    private Date startTime;

    @TableField(value = "end_time")
    private Date endTime;

    /**
     * 执行时长（秒），数据库端为生成列（VIRTUAL）。
     * 避免在 insert/update 中写入该字段。
     */
    @TableField(
            value = "duration_seconds",
            insertStrategy = FieldStrategy.NEVER,
            updateStrategy = FieldStrategy.NEVER
    )
    private Integer durationSeconds;

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

