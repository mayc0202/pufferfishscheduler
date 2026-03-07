package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * 转换分组表单
 *
 * @author mayc
 * @since 2026-03-04
 */
@Data
public class TransGroupForm {

    /**
     * 分组ID
     */
    private Integer id;

    /**
     * 分组名称
     */
    @NotBlank(message = "分组名称不能为空")
    private String name;

    /**
     * 排序字段
     */
    @NotNull(message = "排序字段不能为空")
    private Integer orderBy;

    /**
     * 上级分组ID
     */
    private Integer parentId;

}
