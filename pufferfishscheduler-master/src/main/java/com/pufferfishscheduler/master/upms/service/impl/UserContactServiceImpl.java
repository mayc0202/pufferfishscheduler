package com.pufferfishscheduler.master.upms.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.UserContact;
import com.pufferfishscheduler.dao.mapper.UserContactMapper;
import com.pufferfishscheduler.domain.form.user.UserContactForm;
import com.pufferfishscheduler.domain.vo.user.UserContactVo;
import com.pufferfishscheduler.master.common.dict.service.DictService;
import com.pufferfishscheduler.master.upms.service.UserContactService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 用户中心-联系人
 */
@Service
public class UserContactServiceImpl extends ServiceImpl<UserContactMapper, UserContact> implements UserContactService {

    private static final int MAX_ALERT_METHODS_LENGTH = 256;

    @Autowired
    private DictService dictService;
    
    @Autowired
    private UserContactMapper userContactMapper;

    /**
     * 分页查询联系人
     */
    @Override
    public IPage<UserContactVo> page(String name, Integer pageNo, Integer pageSize) {
        LambdaQueryWrapper<UserContact> queryWrapper = new LambdaQueryWrapper<>();

        if (StringUtils.isNotBlank(name)) {
            queryWrapper.like(UserContact::getName, name.trim());
        }

        queryWrapper.eq(UserContact::getDeleted, Constants.DELETE_FLAG.FALSE)
                .orderByDesc(UserContact::getCreatedTime);

        Page<UserContact> page = new Page<>(pageNo, pageSize);
        IPage<UserContact> raw = userContactMapper.selectPage(page, queryWrapper);

        Page<UserContactVo> out = new Page<>(raw.getCurrent(), raw.getSize(), raw.getTotal());
        out.setRecords(raw.getRecords().stream()
                .map(this::toVo)
                .collect(Collectors.toList()));
        return out;
    }

    /**
     * 查询联系人详情
     */
    @Override
    public UserContactVo detail(Integer id) {
        UserContact row = getExistingUserContact(id);
        return toVo(row);
    }

    /**
     * 新增联系人
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void add(UserContactForm form) {
        if (form.getId() != null) {
            throw new BusinessException("新增联系人请勿传 id");
        }

        String name = form.getName().trim();
        String phone = form.getPhone().trim();
        String email = form.getEmail().trim();

        // 校验唯一性（姓名、手机号、邮箱都不能重复）
        assertUniqueFields(name, phone, email, null);

        Date now = new Date();
        String operator = UserContext.getCurrentAccount();

        UserContact entity = UserContact.builder()
                .name(name)
                .phone(phone)
                .email(email)
                .alertMethods(encodeAlertMethods(form.getAlertMethods()))
                .remark(StringUtils.defaultIfBlank(form.getRemark(), ""))
                .createdBy(operator)
                .createdTime(now)
                .deleted(Constants.DELETE_FLAG.FALSE)
                .build();

        userContactMapper.insert(entity);
    }

    /**
     * 更新联系人
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void update(UserContactForm form) {
        if (form.getId() == null) {
            throw new BusinessException("编辑联系人 id 不能为空!");
        }

        UserContact entity = getExistingUserContact(form.getId());

        String name = form.getName().trim();
        String phone = form.getPhone().trim();
        String email = form.getEmail().trim();

        // 校验唯一性（排除当前ID）
        assertUniqueFields(name, phone, email, form.getId());

        Date now = new Date();
        String operator = UserContext.getCurrentAccount();

        entity.setName(name);
        entity.setPhone(phone);
        entity.setEmail(email);
        entity.setAlertMethods(encodeAlertMethods(form.getAlertMethods()));
        entity.setRemark(StringUtils.defaultIfBlank(form.getRemark(), ""));
        entity.setUpdatedBy(operator);
        entity.setUpdatedTime(now);

        userContactMapper.updateById(entity);
    }

    /**
     * 删除联系人
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void delete(Integer id) {
        // 校验联系人是否存在
        getExistingUserContact(id);

        UpdateWrapper<UserContact> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", id)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());

        userContactMapper.update(null, updateWrapper);
    }

    /**
     * 获取存在的联系人
     */
    private UserContact getExistingUserContact(Integer id) {
        Optional<UserContact> userContact = getOptionalUserContact(id);
        return userContact.orElseThrow(() -> new BusinessException("联系人不存在!"));
    }

    /**
     * 获取联系人（可选）
     */
    private Optional<UserContact> getOptionalUserContact(Integer id) {
        LambdaQueryWrapper<UserContact> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(UserContact::getId, id)
                .eq(UserContact::getDeleted, Constants.DELETE_FLAG.FALSE);
        return Optional.ofNullable(userContactMapper.selectOne(queryWrapper));
    }

    /**
     * 校验姓名、手机号、邮箱的唯一性
     *
     * @param name 联系人姓名
     * @param phone 手机号
     * @param email 邮箱
     * @param excludeId 排除的ID（更新时使用）
     */
    private void assertUniqueFields(String name, String phone, String email, Integer excludeId) {
        LambdaQueryWrapper<UserContact> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(UserContact::getDeleted, Constants.DELETE_FLAG.FALSE);

        // 构建或条件：姓名相同 OR 手机号相同 OR 邮箱相同
        queryWrapper.and(wrapper ->
                wrapper.eq(UserContact::getName, name)
                        .or()
                        .eq(UserContact::getPhone, phone)
                        .or()
                        .eq(UserContact::getEmail, email)
        );

        if (excludeId != null) {
            queryWrapper.ne(UserContact::getId, excludeId);
        }

        List<UserContact> existingContacts = userContactMapper.selectList(queryWrapper);

        if (!existingContacts.isEmpty()) {
            // 构建详细的冲突信息
            StringBuilder conflictMsg = new StringBuilder();

            for (UserContact contact : existingContacts) {
                if (contact.getName().equals(name)) {
                    conflictMsg.append("姓名已存在; ");
                }
                if (contact.getPhone().equals(phone)) {
                    conflictMsg.append("手机号已存在; ");
                }
                if (contact.getEmail().equals(email)) {
                    conflictMsg.append("邮箱已存在; ");
                }
            }

            throw new BusinessException(conflictMsg.toString().trim());
        }
    }

    /**
     * 编码预警方式列表
     */
    private String encodeAlertMethods(List<String> methods) {
        if (methods == null || methods.isEmpty()) {
            return "";
        }

        Set<String> normalized = methods.stream()
                .filter(StringUtils::isNotBlank)
                .map(raw -> dictService.getDictItemValue(Constants.DICT.ALERT_METHOD, raw))
                .peek(code -> {
                    if (StringUtils.isBlank(code)) {
                        throw new BusinessException("不支持的预警方式!");
                    }
                })
                .collect(Collectors.toSet());

        String joined = String.join(",", normalized);
        if (joined.length() > MAX_ALERT_METHODS_LENGTH) {
            throw new BusinessException("预警方式组合过长!");
        }

        return joined;
    }

    /**
     * 解码预警方式字符串
     */
    private List<String> decodeAlertMethods(String stored) {
        if (StringUtils.isBlank(stored)) {
            return Collections.emptyList();
        }

        return Arrays.stream(stored.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }

    /**
     * 转换实体为VO
     */
    private UserContactVo toVo(UserContact entity) {
        if (entity == null) {
            return null;
        }

        UserContactVo vo = new UserContactVo();
        BeanUtils.copyProperties(entity, vo);
        vo.setAlertMethods(decodeAlertMethods(entity.getAlertMethods()));
        return vo;
    }
}