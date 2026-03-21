package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * 转换流程配置表单
 *
 * @author Mayc
 * @since 2026-03-04  17:44
 */
@Data
public class TransFlowConfigForm {

    /**
     * 转换流程配置id
     */
    @NotNull(message = "转换流程配置id不能为空！")
    private Integer id;

    /**
     * 转换配置
     */
    private String config;

    /**
     * 转换图片
     */
    private String img;
}
