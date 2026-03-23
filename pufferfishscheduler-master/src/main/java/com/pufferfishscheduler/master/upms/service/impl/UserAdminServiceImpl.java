package com.pufferfishscheduler.master.upms.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.RSAUtil;
import com.pufferfishscheduler.dao.entity.User;
import com.pufferfishscheduler.dao.entity.UserRoleRel;
import com.pufferfishscheduler.dao.mapper.UserRoleRelMapper;
import com.pufferfishscheduler.domain.form.user.UserManageForm;
import com.pufferfishscheduler.domain.vo.user.RoleOptionVo;
import com.pufferfishscheduler.domain.vo.user.RoleVo;
import com.pufferfishscheduler.domain.vo.user.UserAdminVo;
import com.pufferfishscheduler.domain.vo.user.UserRoleRowVo;
import com.pufferfishscheduler.master.auth.service.AuthService;
import com.pufferfishscheduler.master.upms.service.RoleService;
import com.pufferfishscheduler.master.upms.service.UserAdminService;
import com.pufferfishscheduler.master.upms.service.UserService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 用户管理实现
 */
@Service
public class UserAdminServiceImpl implements UserAdminService {

    @Autowired
    private UserService userService;

    @Autowired
    private RoleService roleService;

    @Autowired
    private UserRoleRelMapper userRoleRelMapper;

    @Autowired
    private AESUtil aesUtil;

    @Autowired
    private RSAUtil rsaUtil;

    @Autowired
    private AuthService authService;

    /**
     * 分页查询用户
     *
     * @param account 账号
     * @param name  姓名
     * @param pageNo 页码
     * @param pageSize 每页数量
     * @return 用户分页列表
     */
    @Override
    public IPage<UserAdminVo> page(String account, String name, Integer pageNo, Integer pageSize) {
        requireAdmin();

        LambdaQueryWrapper<User> q = new LambdaQueryWrapper<>();
        q.eq(User::getDeleted, Constants.DELETE_FLAG.FALSE);
        if (StringUtils.isNotBlank(account)) {
            q.like(User::getAccount, account.trim());
        }
        if (StringUtils.isNotBlank(name)) {
            q.like(User::getName, name.trim());
        }
        q.orderByDesc(User::getCreatedTime);

        Page<User> pageParam = new Page<>(pageNo, pageSize);
        Page<User> raw = userService.page(pageParam, q);

        List<Integer> userIds = raw.getRecords().stream().map(User::getId).toList();
        Map<Integer, List<UserRoleRowVo>> roleByUser = groupRolesByUser(userIds);

        Page<UserAdminVo> out = new Page<>(raw.getCurrent(), raw.getSize(), raw.getTotal());
        out.setRecords(raw.getRecords().stream()
                .map(u -> toAdminVo(u, roleByUser.getOrDefault(u.getId(), Collections.emptyList())))
                .toList());
        return out;
    }

    /**
     * 用户详情
     *
     * @param id 用户id
     * @return 用户详情
     */
    @Override
    public UserAdminVo detail(Integer id) {
        requireAdmin();
        User user = getActiveUserOrThrow(id);
        List<UserRoleRowVo> rows = roleService.listRolesByUserIds(List.of(id));
        return toAdminVo(user, rows);
    }

    /**
     * 列出可分配角色
     *
     * @return 角色列表
     */
    @Override
    public List<RoleVo> listAssignableRoles() {
        requireAdmin();
        return roleService.listAssignableRoles();
    }

    /**
     * 列出可分配角色（给前端下拉框使用）
     */
    @Override
    public List<RoleOptionVo> listAssignableRoleOptions() {
        requireAdmin();
        return roleService.listAssignableRoles().stream()
                .map(r -> {
                    RoleOptionVo vo = new RoleOptionVo();
                    vo.setRoleId(r.getRoleId());
                    vo.setRoleName(r.getRoleName());
                    vo.setRoleDesc(r.getRoleDesc());
                    vo.setDisabled(r.getDisabled());
                    return vo;
                })
                .toList();
    }

    /**
     * 新增用户
     *
     * @param form 用户管理表单
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void add(UserManageForm form) {
        requireAdmin();
        if (form.getId() != null) {
            throw new BusinessException("新增用户请勿传 id");
        }
        if (StringUtils.isBlank(form.getPassword())) {
            throw new BusinessException("新增用户密码不能为空!");
        }

        LambdaQueryWrapper<User> dup = new LambdaQueryWrapper<>();
        dup.eq(User::getAccount, form.getAccount().trim()).eq(User::getDeleted, Constants.DELETE_FLAG.FALSE);
        if (userService.count(dup) > 0) {
            throw new BusinessException("账号已存在!");
        }

        validateRoleIds(form.getRoleIds());

        Date now = new Date();
        String operator = UserContext.getCurrentAccount();

        User user = User.builder()
                .account(form.getAccount().trim())
                .password(encryptPasswordForStore(form.getPassword()))
                .name(form.getName().trim())
                .phone(StringUtils.trimToNull(form.getPhone()))
                .email(StringUtils.trimToNull(form.getEmail()))
                .wechat(StringUtils.trimToNull(form.getWechat()))
                .avatar(form.getAvatar())
                .expireDate(parseExpireDate(form.getExpireDate()))
                .deleted(Constants.DELETE_FLAG.FALSE)
                .createdBy(operator)
                .createdTime(now)
                .build();

        userService.save(user);
        replaceUserRoles(user.getId(), form.getRoleIds(), operator);
    }

    /**
     * 更新用户
     *
     * @param form 用户管理表单
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void update(UserManageForm form) {
        requireAdmin();
        if (form.getId() == null) {
            throw new BusinessException("编辑用户 id 不能为空!");
        }

        User user = getActiveUserOrThrow(form.getId());
        if (!user.getAccount().equals(form.getAccount().trim())) {
            throw new BusinessException("不允许修改登录账号!");
        }

        validateRoleIds(form.getRoleIds());
        assertNotSelfDemoteAdmin(user.getId(), form.getRoleIds());

        Date now = new Date();
        String operator = UserContext.getCurrentAccount();

        user.setName(form.getName().trim());
        user.setPhone(StringUtils.trimToNull(form.getPhone()));
        user.setEmail(StringUtils.trimToNull(form.getEmail()));
        user.setWechat(StringUtils.trimToNull(form.getWechat()));
        user.setAvatar(form.getAvatar());
        user.setExpireDate(parseExpireDate(form.getExpireDate()));
        user.setUpdatedBy(operator);
        user.setUpdatedTime(now);

        if (StringUtils.isNotBlank(form.getPassword())) {
            user.setPassword(encryptPasswordForStore(form.getPassword()));
        }

        userService.updateById(user);
        replaceUserRoles(user.getId(), form.getRoleIds(), operator);
    }

    /**
     * 注销用户
     *
     * @param id 用户id
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deactivate(Integer id) {
        requireAdmin();
        if (Objects.equals(UserContext.getCurrentUserId(), id)) {
            throw new BusinessException("不能注销当前登录账号!");
        }

        // 注销用户
        userService.deactivate(id);

        authService.invalidateUserSessions(id);
    }

    /**
     * 验证当前用户是否为管理员
     */
    private void requireAdmin() {
        Integer uid = UserContext.getCurrentUserId();
        if (uid == null) {
            throw new BusinessException("用户未登录!");
        }
        List<RoleVo> roles = roleService.getRoleListByUserId(uid);
        boolean admin = roles.stream()
                .anyMatch(r -> Constants.ROLE_NAME.ADMIN.equals(r.getRoleName()));
        if (!admin) {
            throw new BusinessException("仅管理员可操作!");
        }
    }

    /**
     * 获取用户信息
     *
     * @param id
     * @return
     */
    private User getActiveUserOrThrow(Integer id) {
        User user = userService.getById(id);
        if (user == null || Boolean.TRUE.equals(user.getDeleted())) {
            throw new BusinessException("用户不存在或已注销!");
        }
        return user;
    }

    /**
     * 验证角色id列表
     *
     * @param roleIds 角色id列表
     */
    private void validateRoleIds(List<Integer> roleIds) {
        if (roleIds == null || roleIds.isEmpty()) {
            throw new BusinessException("请至少选择一个角色!");
        }
        List<RoleVo> allowed = roleService.listAssignableRoles();
        Set<Integer> allowedIds = allowed.stream().map(RoleVo::getRoleId).collect(Collectors.toSet());
        for (Integer rid : new HashSet<>(roleIds)) {
            if (rid == null) {
                throw new BusinessException("角色 id 不能为空!");
            }
            if (!allowedIds.contains(rid)) {
                throw new BusinessException("无效的角色 id: " + rid);
            }
        }
    }

    /**
     * 验证用户是否取消自己的管理员角色
     *
     * @param targetUserId 目标用户id
     * @param newRoleIds   新角色id列表
     */
    private void assertNotSelfDemoteAdmin(Integer targetUserId, List<Integer> newRoleIds) {
        if (!Objects.equals(UserContext.getCurrentUserId(), targetUserId)) {
            return;
        }
        List<RoleVo> allowed = roleService.listAssignableRoles();
        Map<Integer, String> idToName = allowed.stream()
                .collect(Collectors.toMap(RoleVo::getRoleId, RoleVo::getRoleName, (a, b) -> a));
        boolean stillAdmin = newRoleIds.stream()
                .anyMatch(rid -> Constants.ROLE_NAME.ADMIN.equals(idToName.get(rid)));
        if (!stillAdmin) {
            throw new BusinessException("不能取消自己的管理员角色!");
        }
    }

    /**
     * 加密密码用于存储
     *
     * @param rsaCipher 加密后的密码（RSA 加密）
     * @return 加密后的密码（AES 加密）
     */
    private String encryptPasswordForStore(String rsaCipher) {
        try {
            String plain = rsaUtil.decrypt(rsaCipher);
            return aesUtil.encrypt(plain);
        } catch (Exception e) {
            throw new BusinessException("密码处理失败，请确认已使用登录相同的 RSA 公钥加密密码!");
        }
    }

    /**
     * 解析过期日期
     *
     * @param expireDate 过期日期字符串
     * @return 解析后的日期
     */
    private Date parseExpireDate(String expireDate) {
        if (StringUtils.isBlank(expireDate)) {
            return null;
        }
        try {
            return new SimpleDateFormat(Constants.DEFAULT_DATE_FORMAT).parse(expireDate.trim());
        } catch (ParseException e) {
            throw new BusinessException("过期日期格式应为 yyyy-MM-dd");
        }
    }

    /**
     * 按用户id分角色
     *
     * @param userIds 用户id列表
     * @return 角色分组
     */
    private Map<Integer, List<UserRoleRowVo>> groupRolesByUser(List<Integer> userIds) {
        if (userIds.isEmpty()) {
            return Collections.emptyMap();
        }
        return roleService.listRolesByUserIds(userIds).stream()
                .collect(Collectors.groupingBy(UserRoleRowVo::getUserId));
    }

    /**
     * 转换用户管理员VO
     *
     * @param u 用户
     * @param rows 角色行列表
     * @return 用户管理员VO
     */
    private UserAdminVo toAdminVo(User u, List<UserRoleRowVo> rows) {
        UserAdminVo vo = new UserAdminVo();
        BeanUtils.copyProperties(u, vo);
        if (rows == null || rows.isEmpty()) {
            vo.setRoleIds(Collections.emptyList());
            vo.setRoleNames(Collections.emptyList());
        } else {
            vo.setRoleIds(rows.stream().map(UserRoleRowVo::getRoleId).toList());
            vo.setRoleNames(rows.stream().map(UserRoleRowVo::getRoleDesc).toList());
        }
        return vo;
    }

    /**
     * 替换用户角色
     *
     * @param userId 用户id
     * @param roleIds 角色id列表
     * @param operator 操作人
     */
    private void replaceUserRoles(Integer userId, List<Integer> roleIds, String operator) {
        Date now = new Date();
        Set<Integer> newSet = roleIds.stream().collect(Collectors.toSet());

        LambdaQueryWrapper<UserRoleRel> activeQ = new LambdaQueryWrapper<>();
        activeQ.eq(UserRoleRel::getUserId, userId).eq(UserRoleRel::getDeleted, Constants.DELETE_FLAG.FALSE);
        List<UserRoleRel> active = userRoleRelMapper.selectList(activeQ);
        Set<Integer> activeRoleIds = active.stream().map(UserRoleRel::getRoleId).collect(Collectors.toSet());

        for (UserRoleRel rel : active) {
            if (!newSet.contains(rel.getRoleId())) {
                rel.setDeleted(true);
                rel.setUpdatedBy(operator);
                rel.setUpdatedTime(now);
                userRoleRelMapper.updateById(rel);
            }
        }

        for (Integer rid : newSet) {
            if (activeRoleIds.contains(rid)) {
                continue;
            }
            LambdaQueryWrapper<UserRoleRel> oneQ = new LambdaQueryWrapper<>();
            oneQ.eq(UserRoleRel::getUserId, userId).eq(UserRoleRel::getRoleId, rid);
            List<UserRoleRel> existList = userRoleRelMapper.selectList(oneQ);
            UserRoleRel existing = existList.isEmpty() ? null : existList.get(0);
            if (existing != null) {
                existing.setDeleted(false);
                existing.setUpdatedBy(operator);
                existing.setUpdatedTime(now);
                userRoleRelMapper.updateById(existing);
            } else {
                UserRoleRel row = UserRoleRel.builder()
                        .userId(userId)
                        .roleId(rid)
                        .deleted(Constants.DELETE_FLAG.FALSE)
                        .createdBy(operator)
                        .createdTime(now)
                        .build();
                userRoleRelMapper.insert(row);
            }
        }
    }
}
