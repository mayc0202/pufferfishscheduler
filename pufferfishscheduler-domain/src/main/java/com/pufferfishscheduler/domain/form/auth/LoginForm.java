package com.pufferfishscheduler.domain.form.auth;

import lombok.Data;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.NotBlank;

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
    private String username;

    @NotBlank(message = "密码不能为空!")
    private String password;

}