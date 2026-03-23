package com.pufferfishscheduler.master.upms.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.dao.entity.User;

/**
 * (User) Service
 *
 * @author mayc
 * @since 2025-05-23 00:24:51
 */
public interface UserService extends IService<User> {

    /**
     * 获取用户信息
     *
     * @param id
     * @param account
     * @return
     */
    User getOneUserByIdAndAccount(Integer id, String account);

    /**
     * 校验用户是否存在
     *
     * @param id
     * @param account
     */
    void verifyUserIsExist(Integer id, String account);

    /**
     * 注销用户（逻辑删除）
     *
     * @param id
     */
    void deactivate(Integer id);
}

