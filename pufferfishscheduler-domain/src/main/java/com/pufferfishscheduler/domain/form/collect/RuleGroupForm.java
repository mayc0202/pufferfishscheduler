package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

/**
 * 规则分类表单
 */
@Data
public class RuleGroupForm {

    /**
     * 分类ID（新增可为空）
     */
    private Integer id;

    /**
     * 分类名称
     */
    @NotBlank(message = "分类名称不能为空!")
    @Size(max = 50, message = "分类名称长度不能超过50字符!")
    private String groupName;

    /**
     * 上级分类ID（顶级可为空）
     */
    private Integer parentId;

    /**
     * 排序
     */
    @NotNull(message = "排序不能为空!")
    private Integer orderBy;
}
