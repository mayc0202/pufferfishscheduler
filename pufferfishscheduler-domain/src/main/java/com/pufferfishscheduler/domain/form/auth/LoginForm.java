package com.pufferfishscheduler.domain.form.auth;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

/**
 * @Author: yc
 * @CreateTime: 2025-05-23
 * @Description: Login Information
 * @Version: 1.0
 */
@Data
public class LoginForm {

    @NotBlank(message = "账号不能为空!")
    @Size(max = 32, message = "账号长度不能超过32字符!")
    @ApiModelProperty(value = "账号", name = "account", example = "mayc")
    private String account;

    @NotBlank(message = "密码不能为空!")
    @ApiModelProperty(value = "密码", name = "password", example = "123456")
    private String password;

}