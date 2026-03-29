package com.pufferfishscheduler.domain.vo.collect;

import lombok.Data;

/**
 * 规则处理器详情
 */
@Data
public class RuleProcessorVo {

    private Integer id;

    private String processorName;

    private String paramsConfig;

    private String processorClass;

    private Integer styleType;
}
