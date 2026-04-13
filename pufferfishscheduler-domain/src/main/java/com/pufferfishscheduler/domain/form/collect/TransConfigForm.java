package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * 配置表单
 */
@Data
public class TransConfigForm {

    /**
     * 流程ID
     */
    private Integer flowId;

    @NotBlank(message = "流程不能为空！")
    private String config;

    private String stepName;
}
