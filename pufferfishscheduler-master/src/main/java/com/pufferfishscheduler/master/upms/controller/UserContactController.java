package com.pufferfishscheduler.master.upms.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.user.UserContactForm;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import com.pufferfishscheduler.master.upms.service.UserContactService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 用户中心-联系人管理（需登录且为管理员角色，风格对齐用户管理）
 */
@Tag(name = OpenApiTags.USER_CONTACT, description = OpenApiTags.USER_CONTACT_DESC)
@Validated
@RestController
@RequestMapping(value = "/user/contact", produces = {"application/json;charset=utf-8"})
public class UserContactController {

    @Autowired
    private UserContactService userContactService;

    @Operation(summary = "分页查询联系人（姓名模糊）")
    @GetMapping("/list.do")
    public ApiResponse list(
            @RequestParam(required = false) String name,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse.success(userContactService.page(name, pageNo, pageSize));
    }

    @Operation(summary = "联系人详情")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam Integer id) {
        return ApiResponse.success(userContactService.detail(id));
    }

    @Operation(summary = "新增联系人（预警方式多选：alertMethods 传字符串列表，如 [\"0\",\"1\"]）")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid UserContactForm form) {
        userContactService.add(form);
        return ApiResponse.success("联系人创建成功!");
    }

    @Operation(summary = "编辑联系人")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid UserContactForm form) {
        userContactService.update(form);
        return ApiResponse.success("联系人更新成功!");
    }

    @Operation(summary = "删除联系人")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam Integer id) {
        userContactService.delete(id);
        return ApiResponse.success("联系人已删除!");
    }
}
