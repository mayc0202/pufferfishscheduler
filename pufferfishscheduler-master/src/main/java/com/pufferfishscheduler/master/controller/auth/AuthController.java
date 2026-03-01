package com.pufferfishscheduler.master.controller.auth;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.auth.LoginForm;
import com.pufferfishscheduler.service.upms.AuthService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 *
 * @author Mayc
 * @since 2025-09-22  00:52
 */
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
    @PostMapping("/login.do")
    public ApiResponse login(@RequestBody @Valid LoginForm loginInfo) {
        return ApiResponse.success(authService.login(loginInfo));
    }

    /**
     * 注销
     *
     * @return
     */
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
    @GetMapping("/getUserInfo.do")
    public ApiResponse getInfo() {
        return ApiResponse.success(authService.getUserInfo(null));
    }

    /**
     * 刷新token
     *
     * @return
     */
    @GetMapping("/refreshToken.do")
    public ApiResponse refreshToken(@RequestParam String token) {
        return ApiResponse.success(authService.refreshToken(token));
    }
}
