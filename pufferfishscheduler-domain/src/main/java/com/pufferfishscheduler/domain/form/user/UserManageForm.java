package com.pufferfishscheduler.domain.form.user;

import com.pufferfishscheduler.domain.annotation.ValidateEmailFormat;
import com.pufferfishscheduler.domain.annotation.ValidatePhoneFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.List;

/**
 * 用户新增/编辑（密码为 RSA 加密后的密文，与登录一致）
 */
@Data
public class UserManageForm {

    /**
     * 编辑时必填
     */
    private Integer id;

    @NotBlank(message = "账号不能为空!")
    @Size(max = 32, message = "账号长度不能超过32!")
    private String account;

    /**
     * RSA 密文；新增时必填，编辑时留空表示不修改密码
     */
    private String password;

    @NotBlank(message = "姓名不能为空!")
    @Size(max = 64, message = "姓名长度不能超过64!")
    private String name;

    @Size(max = 11, message = "手机号长度不能超过11!")
    @ValidatePhoneFormat(
            message = "请输入正确的手机号码",
            allowEmpty = true
    )
    private String phone;

    @Size(max = 64, message = "邮箱长度不能超过64!")
    @ValidateEmailFormat(
            message = "请输入正确的邮箱",
            allowEmpty = true
    )
    private String email;

    @Size(max = 64, message = "微信长度不能超过64!")
    private String wechat;

    private String avatar;

    /**
     * 过期日期，格式 yyyy-MM-dd，可空表示不过期
     */
    private String expireDate;

    @NotEmpty(message = "请至少选择一个角色!")
    private List<Integer> roleIds;
}
