package com.pufferfishscheduler.domain.vo.collect;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.util.Date;

/**
 * 转换任务列表/详情 VO
 *
 * @author Mayc
 * @since 2026-03-20
 */
@Data
public class TransTaskVo {

    private Integer id;

    private String name;

    private String remark;

    private Integer flowId;

    private String flowName;

    private Integer groupId;

    private String groupName;

    private String executeType;

    /** 调度方式文本 */
    private String executeTypeTxt;

    private String cron;

    private String failurePolicy;

    private String failurePolicyTxt;

    private String notifyPolicy;

    private String notifyPolicyTxt;

    private Boolean enable;

    private String enableTxt;

    private String status;

    private String statusTxt;

    private String reason;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date executeTime;

    private String executeTimeTxt;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date createdTime;

    private String createdTimeTxt;
}
