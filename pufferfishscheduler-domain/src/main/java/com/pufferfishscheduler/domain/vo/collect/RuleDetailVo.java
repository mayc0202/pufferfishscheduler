package com.pufferfishscheduler.domain.vo.collect;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 规则详情VO
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class RuleDetailVo extends RuleVo {

    private String config;
}
