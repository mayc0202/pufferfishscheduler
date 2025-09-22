package com.pufferfishscheduler.service.upms.user.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.User;
import com.pufferfishscheduler.dao.mapper.UserMapper;
import com.pufferfishscheduler.service.upms.user.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 *
 * @author Mayc
 * @since 2025-09-21  03:22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements UserService {

    @Autowired
    private UserMapper userMapper;

    /**
     * 获取用户信息
     *
     * @param id
     * @param account
     * @return
     */
    @Override
    public User getOneUserByIdAndAccount(Integer id, String account) {
        LambdaQueryWrapper<User> queryWrapper = buildQueryWrapper(id, account);
        return userMapper.selectOne(queryWrapper);
    }

    /**
     * 校验用户是否存在
     *
     * @param id
     * @param account
     */
    @Override
    public void verifyUserIsExist(Integer id, String account) {
        LambdaQueryWrapper<User> queryWrapper = buildQueryWrapper(id, account);
        User user = userMapper.selectOne(queryWrapper);
        if (Objects.isNull(user)) {
            throw new BusinessException("请校验用户是否存在!");
        }
    }

    /**
     * 构建查询条件
     *
     * @param id
     * @param account
     * @return
     */
    private LambdaQueryWrapper<User> buildQueryWrapper(Integer id, String account) {
        LambdaQueryWrapper<User> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(User::getDeleted, Constants.DELETE_FLAG.FALSE);

        if (Objects.nonNull(id)) {
            queryWrapper.eq(User::getId, id);
        }

        if (Objects.nonNull(account)) {
            queryWrapper.eq(User::getAccount, account);
        }
        return queryWrapper;
    }
}
