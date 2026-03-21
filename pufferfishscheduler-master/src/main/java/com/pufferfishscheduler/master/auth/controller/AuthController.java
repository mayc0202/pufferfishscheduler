package com.pufferfishscheduler.master.auth.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.auth.LoginForm;
import com.pufferfishscheduler.master.auth.service.AuthService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * 认证控制器
 *
 * @author Mayc
 * @since 2025-09-22  00:52
 */
@Tag(name = OpenApiTags.AUTH, description = OpenApiTags.AUTH_DESC)
@Validated
@RestController
@RequestMapping(value = "/auth", produces = {"application/json;charset=utf-8"})
public class AuthController {

    @Autowired
    private AuthService authService;

    /**
     * 获取认证信息
     *
     * @return
     */
    @Operation(summary = "获取认证信息")
    @GetMapping("/getAuth.do")
    public ApiResponse getPublicKey() {
        return ApiResponse.success(authService.getAuth());
    }

    /**
     * 登录
     *
     * @param loginInfo
     * @return
     */
    @Operation(summary = "登录")
    @PostMapping("/login.do")
    public ApiResponse login(@RequestBody @Valid LoginForm loginInfo) {
        return ApiResponse.success(authService.login(loginInfo));
    }

    /**
     * 注销
     *
     * @return
     */
    @Operation(summary = "注销")
    @PostMapping("/logout.do")
    public ApiResponse logout() {
        authService.logout();
        return ApiResponse.success("用户注销成功!");
    }

    /**
     * 获取用户信息
     *
     * @return
     */
    @Operation(summary = "获取用户信息")
    @GetMapping("/getUserInfo.do")
    public ApiResponse getInfo() {
        return ApiResponse.success(authService.getUserInfo(null));
    }

    /**
     * 刷新token
     *
     * @return
     */
    @Operation(summary = "刷新token")
    @GetMapping("/refreshToken.do")
    public ApiResponse refreshToken(@RequestParam String token) {
        return ApiResponse.success(authService.refreshToken(token));
    }
}
