package com.pufferfishscheduler.master.auth.service;

import com.pufferfishscheduler.domain.form.auth.LoginForm;
import com.pufferfishscheduler.domain.vo.user.AuthVo;
import com.pufferfishscheduler.domain.vo.user.UserVo;

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

    /**
     * 刷新token
     *
     * @param oldToken
     * @return
     */
    String refreshToken(String oldToken);

    /**
     * 使用户当前登录态失效（列入黑名单并清理 SSO），用于管理员注销用户等场景
     */
    void invalidateUserSessions(Integer userId);
}
