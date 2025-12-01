package com.pufferfishscheduler.domain.vo.metadata;

import com.alibaba.fastjson2.annotation.JSONField;
import com.pufferfishscheduler.domain.DomainConstants;
import lombok.Data;

import java.util.Date;

/**
 *
 * @author Mayc
 * @since 2025-11-27  14:52
 */
@Data
public class MetadataTaskVo {

    private Integer id;

    /**
     * 数据源id
     */
    private Integer dbId;

    /**
     * 数据源名称
     */
    private String dbName;

    /**
     * 数据源分组id
     */
    private Integer dbGroupId;

    /**
     * 数据源分组名称
     */
    private String dbGroupName;

    /**
     * cron执行表达式
     */
    private String cron;

    /**
     * 计划执行错误策略（0继续 1结束）
     */
    private String failurePolicy;
    private String failurePolicyTxt;

    /**
     * 通知策略（0都不发 1成功时通知 2失败时通知 3全部通知）
     */
    private String notifyPolicy;
    private String notifyPolicyTxt;

    /**
     * 禁用状态（0启用 1禁用）
     */
    private Boolean enable;
    private String enableTxt;

    /**
     * 状态 0-未启动 1-启动中 2-运行中 3-成功 4-失败 5-停止中 6-已停止 7-异常
     */
    private String status;
    private String statusTxt;

    /**
     * 备注
     */
    private String remark;

    /**
     * 工作组id
     */
    private Integer workerId;
    private String workerName;

    /**
     * 执行时间
     */
    @JSONField(format = DomainConstants.DEFAULT_DATE_TIME_FORMAT)
    private Date executeTime;
    private String executeTimeTxt;

    /**
     * 创建时间
     */
    @JSONField(format = DomainConstants.DEFAULT_DATE_TIME_FORMAT)
    private Date createdTime;
    private String createdTimeTxt;
}
