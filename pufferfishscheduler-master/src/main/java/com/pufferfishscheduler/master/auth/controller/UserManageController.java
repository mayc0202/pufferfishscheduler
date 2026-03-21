package com.pufferfishscheduler.master.auth.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.user.UserManageForm;
import com.pufferfishscheduler.master.auth.service.UserAdminService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
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
 * 用户管理（需登录且为管理员角色）
 */
@Tag(name = OpenApiTags.USER_ADMIN, description = OpenApiTags.USER_ADMIN_DESC)
@Validated
@RestController
@RequestMapping(value = "/user/manage", produces = {"application/json;charset=utf-8"})
public class UserManageController {

    @Autowired
    private UserAdminService userAdminService;

    @Operation(summary = "分页查询用户")
    @GetMapping("/list.do")
    public ApiResponse list(
            @RequestParam(required = false) String account,
            @RequestParam(required = false) String name,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse.success(userAdminService.page(account, name, pageNo, pageSize));
    }

    @Operation(summary = "用户详情（含角色）")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam Integer id) {
        return ApiResponse.success(userAdminService.detail(id));
    }

    @Operation(summary = "可分配角色列表（管理员/操作员）")
    @GetMapping("/roles.do")
    public ApiResponse roles() {
        return ApiResponse.success(userAdminService.listAssignableRoles());
    }

    @Operation(summary = "可分配角色下拉选项（带 disabled）")
    @GetMapping("/rolesOptions.do")
    public ApiResponse rolesOptions() {
        return ApiResponse.success(userAdminService.listAssignableRoleOptions());
    }

    @Operation(summary = "新增用户并赋权")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid UserManageForm form) {
        userAdminService.add(form);
        return ApiResponse.success("用户创建成功!");
    }

    @Operation(summary = "编辑用户与角色")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid UserManageForm form) {
        userAdminService.update(form);
        return ApiResponse.success("用户更新成功!");
    }

    @Operation(summary = "注销用户（逻辑删除）")
    @PutMapping("/deactivate.do")
    public ApiResponse deactivate(@RequestParam Integer id) {
        userAdminService.deactivate(id);
        return ApiResponse.success("用户已注销!");
    }
}
