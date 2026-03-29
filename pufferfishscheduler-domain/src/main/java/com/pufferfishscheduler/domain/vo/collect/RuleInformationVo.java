package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 规则信息
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RuleInformationVo {

    /**
     * 规则ID
     */
    private String id;

    /**
     * 规则名称
     */
    private String name;

    /**
     * 规则配置
     */
    private String config;

    /**
     * 处理器ID
     */
    private Integer processorId;

    /**
     * 处理器类全名
     */
    private String processorClass;
}
