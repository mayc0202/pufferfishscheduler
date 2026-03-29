package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * 规则表单
 */
@Data
public class RuleForm {

    private String id;

    @NotNull(message = "规则分类不能为空")
    private Integer groupId;

    @NotBlank(message = "规则名称不能为空")
    private String ruleName;

    private String ruleCode;

    @NotNull(message = "规则处理器不能为空")
    private Integer ruleProcessorId;

    private String ruleDescription;

    /**
     * false-草稿 true-发布
     */
    private Boolean status;

    private Boolean deleted;

    @NotBlank(message = "规则配置不能为空")
    private String config;
}
