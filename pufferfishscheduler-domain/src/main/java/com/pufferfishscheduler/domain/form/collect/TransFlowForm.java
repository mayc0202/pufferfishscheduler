package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

/**
 * 转换流表单
 */
@Data
public class TransFlowForm {

    private Integer id;

    /**
     * 转换名称
     */
    @NotBlank(message = "数据流程名称不能为空！")
    @Size(max = 100,message = "数据流程名称不能超过100字！")
    private String name;

    /**
     * 描述
     */
    @Size(max = 500,message = "描述不能超过500字！")
    private String description;

    private String path;

    /**
     * 分类id
     */
    @NotNull(message = "分组不能为空！")
    private Integer groupId;

    private String sceneType = "1";

    private Integer rowSize = 2000;
}
