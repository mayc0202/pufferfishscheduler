package com.pufferfishscheduler.domain.form.user;

import com.pufferfishscheduler.domain.annotation.ValidateEmailFormat;
import com.pufferfishscheduler.domain.annotation.ValidatePhoneFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 用户中心-联系人新增/编辑（预警方式多选，提交为列表，持久化为逗号分隔）
 */
@Data
public class UserContactForm {

    /**
     * 编辑时必填
     */
    private Integer id;

    @NotBlank(message = "姓名不能为空!")
    @Size(max = 64, message = "姓名长度不能超过64!")
    private String name;

    @NotBlank(message = "手机号不能为空!")
    @Size(max = 11, message = "手机号长度不能超过11!")
    @ValidatePhoneFormat(
            message = "请输入正确的手机号码",
            allowEmpty = false
    )
    private String phone;

    @NotBlank(message = "邮箱不能为空!")
    @Size(max = 128, message = "邮箱长度不能超过128!")
    @ValidateEmailFormat(
            message = "请输入正确的邮箱",
            allowEmpty = false
    )
    private String email;

    /**
     * 预警方式多选，如 SMS、EMAIL；可为空表示不启用预警
     */
    private List<String> alertMethods = new ArrayList<>();

    @Size(max = 500, message = "备注长度不能超过500!")
    private String remark;
}
