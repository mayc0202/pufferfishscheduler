package com.pufferfishscheduler.master.auth.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.auth.LoginForm;
import com.pufferfishscheduler.master.auth.service.AuthService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.validation.Valid;

/**
 *
 * @author Mayc
 * @since 2025-09-22  00:52
 */
@Api(tags = "用户信息认证管理")
@Validated
@RestController
@RequestMapping(value = "/auth", produces = {"application/json;charset=utf-8"})
public class AuthController {

    @Resource
    private AuthService authService;

    /**
     * 获取认证信息
     *
     * @return
     */
    @ApiOperation(value = "获取认证信息")
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
    @ApiOperation(value = "用户登录")
    @PostMapping("/login.do")
    public ApiResponse login(@RequestBody @Valid LoginForm loginInfo) {
        return ApiResponse.success(authService.login(loginInfo));
    }

    /**
     * 注销
     *
     * @return
     */
    @ApiOperation(value = "注销用户")
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
    @ApiOperation(value = "获取用户信息")
    @GetMapping("/getUserInfo.do")
    public ApiResponse getInfo() {
        return ApiResponse.success(authService.getUserInfo(null));
    }

}
