package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * 字段流表单
 * 
 * @author mayc
 */
@Data
public class FieldStreamForm {
    /**
     * 转换流id
     */
    @NotNull(message = "转换流id不能为空")
    private Integer flowId;

    /**
     * 转换流配置
     */
    @NotBlank(message = "转换流配置不能为空")
    private String config;

    /**
     * 转换流步骤名称
     */
    @NotBlank(message = "转换流步骤名称不能为空")
    private String stepName;

    /**
     * 转换组件code
     */
    private String code;

    /**
     * 数据源id
     */
    private Integer dbId;

    /**
     * 表id
     */
    private Integer tableId;
}
