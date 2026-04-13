package com.pufferfishscheduler.domain.form.database;

import lombok.Data;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.Date;

/**
 * @Author: yc
 * @CreateTime: 2025-06-03
 * @Description:
 * @Version: 1.0
 */
@Data
public class DbDatabaseForm {

    private Integer id;

    @NotBlank(message = "数据源名称不能为空!")
    private String name;

    @NotNull(message = "数据源分组不能为空!")
    private Integer groupId;

    @NotBlank(message = "数据源分层不能为空!")
    private String label;

    @NotBlank(message = "数据库大类不能为空!")
    private String category;

    @NotBlank(message = "数据库类型不能为空!")
    private String type;

    @NotBlank(message = "主机地址不能为空!")
    private String dbHost;

    @NotBlank(message = "端口不能为空!")
    private String dbPort;

//    @NotBlank(message = "数据库名称/SID不能为空!")
    private String dbName;

    // 模式schema
    private String dbSchema;

//    @NotBlank(message = "数据库用户名不能为空!")
    private String username;

//    @NotBlank(message = "数据库密码不能为空!")
    private String password;

    // 备注
    private String remark;

    // 最近同步时间
    private Date lastSyncTime;

    //数据同步周期
    private Integer dataSyncCycle;

    // 属性
    private String properties;

    // 扩展配置
    private String extConfig;

    // be地址
    private String beAddress;

    // fe地址
    private String feAddress;

    // 连接方式
    private String connectType;

    // 控制编码
    private String controlEncoding;

    // FTP主动/被动模式
    private String mode;

}