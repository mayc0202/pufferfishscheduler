package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * 预览表单
 */
@Data
public class PreviewForm {
    /**
     * 转换流程id
     */
    @NotNull(message = "转换流程id不能为空！")
    private Integer id;

    /**
     * 转换配置
     */
    @NotBlank(message = "转换配置不能为空！")
    private String config;
}
