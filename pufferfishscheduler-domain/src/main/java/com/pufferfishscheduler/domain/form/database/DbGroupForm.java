package com.pufferfishscheduler.domain.form.database;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;


/**
 * @Author: yc
 * @CreateTime: 2025-05-22
 * @Description:
 * @Version: 1.0
 */
@Data
public class DbGroupForm {

    private Integer id;

    /**
     * 分组名称
     */
    @NotBlank(message = "分组名称不能为空!")
    @Size(max = 50,message = "分组名称长度不能超过50字符!")
    private String name;

    /**
     * 排序
     */
    @NotNull(message = "排序不能为空!")
    private Integer orderBy;

}