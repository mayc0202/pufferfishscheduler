package com.pufferfishscheduler.domain.form.database;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class ResourceForm {

    /**
     * 数据源id
     */
    @NotNull(message = "FTP/FTPS数据源id不能为空!")
    private Integer dbId;

    /**
     * 资源名称
     */
    private String name;

    /**
     * 旧名称
     */
    private String oldName;

    /**
     * 新名称
     */
    private String newName;

    /**
     * 资源类型
     */
    @NotBlank(message = "资源类型不能为空!")
    private String type;

    /**
     * 当前路径
     */
    private String path;

    /**
     * 远程路径
     */
    private String remotePath;

    /**
     * 来源路径
     */
    private String fromPath;

    /**
     * 目标数据
     */
    private String toPath;

}
