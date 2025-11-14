package com.pufferfishscheduler.service.upms.service;

import com.pufferfishscheduler.domain.vo.user.AuthVo;
import com.pufferfishscheduler.domain.vo.user.UserVo;
import com.pufferfishscheduler.domain.form.auth.LoginForm;

/**
 *
 * @author Mayc
 * @since 2025-09-21  18:53
 */
public interface AuthService {

    /**
     * 获取认证信息
     *
     * @return
     */
    AuthVo getAuth();

    /**
     * 登录用户
     *
     * @param loginForm
     * @return
     */
    String login(LoginForm loginForm);

    /**
     * 获取用户信息
     *
     * @param token
     * @return
     */
    UserVo getUserInfo(String token);

    /**
     * 注销账户
     */
    void logout();
}
