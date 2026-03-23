package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 转换任务日志VO
 *
 * @author Mayc
 * @since 2026-03-23  16:08
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TransTaskLogVo {

    /**
     * 批次ID
     */
    private String id;

    /**
     * 任务ID，关联 trans_task.id
     */
    private Integer taskId;
    private String taskName;

    /**
     * 转换流程ID，关联 trans_flow.id
     */
    private Integer flowId;
    private String flowName;

    /**
     * 转换流程配置
     */
    private String flowConfig;

    /**
     * 执行状态：
     * INIT-未启动、RUNNING-运行中、SUCESS-成功、STOP-已停止、FAILURE-异常、STARTING-启动中、STOPPING-停止中
     */
    private String status;
    private String statusTxt;

    /**
     * 执行方式：1-定时执行；2-手动执行
     */
    private String executingType;
    private String executingTypeTxt;

    private Long dataVolume;

    private Long linesRead;

    private Long linesWritten;

    private Long linesUpdated;

    private Long linesInput;

    private Long linesOutput;

    private Long linesRejected;

    private Long linesErrors;

    private String reason;

    /**
     * 转换流程图片
     */
    private String flowImage;

    private String parameters;

    private String logDetail;

    /**
     * 执行开始时间
     */
    private Date startTime;
    private String startTimeTxt;

    /**
     * 执行结束时间
     */
    private Date endTime;
    private String endTimeTxt;

    /**
     * 执行时长（秒），数据库端为生成列（VIRTUAL）。
     * 避免在 insert/update 中写入该字段。
     */
    private Integer durationSeconds;

}
