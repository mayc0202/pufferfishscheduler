package com.pufferfishscheduler.master.collect.rule.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.node.GenericTreeBuilder;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.common.node.TreeNode;
import com.pufferfishscheduler.dao.entity.RuleGroup;
import com.pufferfishscheduler.dao.mapper.RuleGroupMapper;
import com.pufferfishscheduler.domain.form.collect.RuleGroupForm;
import com.pufferfishscheduler.domain.vo.user.RoleVo;
import com.pufferfishscheduler.master.collect.rule.service.RuleGroupService;
import com.pufferfishscheduler.master.upms.service.RoleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 规则组服务实现类
 *
 * @author Mayc
 * @since 2026-03-18  15:59
 */
@Slf4j
@Service
public class RuleGroupServiceImpl implements RuleGroupService {

    @Autowired
    private GenericTreeBuilder treeBuilder;

    @Autowired
    private RuleGroupMapper groupMapper;
    @Autowired
    private RoleService roleService;

    /**
     * 固定分类ID，逗号分隔（可选）
     */
    @Value("${regular.groupId:}")
    private String regularGroupId;
    @Value("${rule.group.public-id:}")
    private String publicGroupIdCfg;
    @Value("${rule.group.custom-id:}")
    private String customGroupIdCfg;

    /**
     * 获取规则分类树
     *
     * @param name 分类名称，用于模糊查询
     * @return 规则分类树菜单
     */
    @Override
    public List<Tree> tree(String name) {
        List<RuleGroup> groups = getGroupList(name);
        List<Tree> allNodes = convertToTreeNodes(groups);
        return treeBuilder.buildTree(allNodes, Comparator.comparing(TreeNode::getOrder));
    }

    /**
     * 新增规则分类
     *
     * @param form 分类表单
     *             分类表单验证规则：分类名称不能重复，不能选择子分类作为父级
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void add(RuleGroupForm form) {
        if (isFixedGroupId(form.getId()) || isFixedGroupId(form.getParentId())) {
            throw new BusinessException("您没有操作固定分类的权限！");
        }
        // 仅 admin 可在公共目录下创建；其他角色只能在自定义目录下创建
        validateGroupCreatePermission(form.getParentId());

        Integer firstGroupId = getFirstGroupId(form.getParentId());
        verifyGroupNameIsRepeat(form.getGroupName(), form.getParentId(), null);

        RuleGroup group = new RuleGroup();
        group.setGroupName(form.getGroupName());
        group.setParentId(form.getParentId());
        group.setOrderBy(form.getOrderBy());
        group.setGroupType(firstGroupId);
        group.setDeleted(Constants.DELETE_FLAG.FALSE);
        group.setCreatedBy(UserContext.getCurrentAccount());
        group.setCreatedTime(new Date());
        groupMapper.insert(group);
    }

    /**
     * 更新规则分类
     *
     * @param form 分类表单
     *             分类表单验证规则：分类名称不能重复，不能选择子分类作为父级
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void update(RuleGroupForm form) {
        RuleGroup group = groupMapper.selectById(form.getId());
        if (group == null || Boolean.TRUE.equals(group.getDeleted())) {
            throw new BusinessException("分类不存在，请刷新重试！");
        }
        if (isFixedGroupId(form.getId()) || isFixedGroupId(form.getParentId())) {
            throw new BusinessException("您没有操作固定分类的权限！");
        }

        if (!Objects.equals(group.getGroupName(), form.getGroupName())) {
            verifyGroupNameIsRepeat(form.getGroupName(), form.getParentId(), form.getId());
        }

        List<Integer> childIds = getAllGroupIdById(form.getId());
        if (childIds.contains(form.getParentId())) {
            throw new BusinessException("不能选择子分类作为父级!");
        }

        Integer firstGroupId = getFirstGroupId(form.getParentId());
        RuleGroup toUpdate = new RuleGroup();
        toUpdate.setId(form.getId());
        toUpdate.setGroupName(form.getGroupName());
        toUpdate.setParentId(form.getParentId());
        toUpdate.setOrderBy(form.getOrderBy());
        toUpdate.setGroupType(firstGroupId);
        toUpdate.setUpdatedBy(UserContext.getCurrentAccount());
        toUpdate.setUpdatedTime(new Date());
        groupMapper.updateById(toUpdate);
    }

    /**
     * 删除规则分类
     *
     * @param id 分类ID
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void delete(Integer id) {
        RuleGroup group = groupMapper.selectById(id);
        if (group == null || Boolean.TRUE.equals(group.getDeleted())) {
            throw new BusinessException("该分类不存在，请刷新重试！");
        }
        if (isFixedGroupId(id)) {
            throw new BusinessException("固定分类不允许删除！");
        }

        LambdaQueryWrapper<RuleGroup> childQw = new LambdaQueryWrapper<>();
        childQw.eq(RuleGroup::getParentId, id)
                .eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        Long childCount = groupMapper.selectCount(childQw);
        if (childCount != null && childCount > 0) {
            throw new BusinessException("当前分组存在子级菜单目录，无法删除！");
        }

        UpdateWrapper<RuleGroup> uw = new UpdateWrapper<>();
        uw.eq("id", id)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());
        groupMapper.update(null, uw);
    }

    /**
     * 获取固定分类ID集合
     *
     * @return 固定分类ID数组
     */
    @Override
    public String[] getRegularGroupId() {
        if (StringUtils.isBlank(regularGroupId)) {
            return new String[0];
        }
        return Arrays.stream(regularGroupId.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .toArray(String[]::new);
    }

    /**
     * 参考 trans_flow 分组查询逻辑：先按条件拉平列表，再构建树。
     */
    private List<RuleGroup> getGroupList(String name) {
        LambdaQueryWrapper<RuleGroup> allQw = new LambdaQueryWrapper<>();
        allQw.eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE)
                .orderByDesc(RuleGroup::getOrderBy)
                .orderByAsc(RuleGroup::getId);
        List<RuleGroup> allGroups = groupMapper.selectList(allQw);

        if (StringUtils.isBlank(name)) {
            return allGroups;
        }

        String keyword = name.trim();
        Set<Integer> keepIds = new LinkedHashSet<>();
        Map<Integer, RuleGroup> groupMap = allGroups.stream()
                .collect(Collectors.toMap(RuleGroup::getId, g -> g, (a, b) -> a));

        for (RuleGroup group : allGroups) {
            if (StringUtils.containsIgnoreCase(group.getGroupName(), keyword)) {
                RuleGroup current = group;
                while (current != null) {
                    keepIds.add(current.getId());
                    Integer pid = current.getParentId();
                    current = pid == null ? null : groupMap.get(pid);
                }
            }
        }
        if (keepIds.isEmpty()) {
            return Collections.emptyList();
        }
        return allGroups.stream().filter(g -> keepIds.contains(g.getId())).collect(Collectors.toList());
    }

    /**
     * 将规则分类列表转换为树节点列表
     *
     * @param groups 规则分类列表
     * @return 树节点列表
     */
    private List<Tree> convertToTreeNodes(List<RuleGroup> groups) {
        List<Tree> nodes = new ArrayList<>();
        for (RuleGroup group : groups) {
            Tree tree = new Tree();
            tree.setId(group.getId());
            tree.setName(group.getGroupName());
            tree.setType(Constants.TREE_TYPE.GROUP);
            tree.setParentId(group.getParentId());
            tree.setOrderBy(group.getOrderBy());
            nodes.add(tree);
        }
        return nodes;
    }

    /**
     * 获取规则分类的第一级分类ID
     *
     * @param parentId 父级分类ID
     * @return 第一级分类ID
     */
    private Integer getFirstGroupId(Integer parentId) {
        if (parentId == null) {
            return null;
        }
        RuleGroup ruleGroup = groupMapper.selectById(parentId);
        if (ruleGroup == null || Boolean.TRUE.equals(ruleGroup.getDeleted())) {
            throw new BusinessException("父级分类不存在，请刷新重试！");
        }
        if (ruleGroup.getParentId() != null) {
            return getFirstGroupId(ruleGroup.getParentId());
        }
        return ruleGroup.getId();
    }

    /**
     * 验证分类名称是否重复
     *
     * @param name      分类名称
     * @param parentId  父级分类ID
     * @param excludeId 排除的分类ID
     */
    private void verifyGroupNameIsRepeat(String name, Integer parentId, Integer excludeId) {
        LambdaQueryWrapper<RuleGroup> qw = new LambdaQueryWrapper<>();
        qw.eq(RuleGroup::getGroupName, name)
                .eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        if (parentId == null) {
            qw.isNull(RuleGroup::getParentId);
        } else {
            qw.eq(RuleGroup::getParentId, parentId);
        }
        if (excludeId != null) {
            qw.ne(RuleGroup::getId, excludeId);
        }
        Long cnt = groupMapper.selectCount(qw);
        if (cnt != null && cnt > 0) {
            throw new BusinessException("同一级下分类名称不能重复!");
        }
    }

    /**
     * 获取所有子分类ID
     *
     * @param id 分类ID
     * @return 所有子分类ID列表
     */
    private List<Integer> getAllGroupIdById(Integer id) {
        LambdaQueryWrapper<RuleGroup> qw = new LambdaQueryWrapper<>();
        qw.eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        List<RuleGroup> groups = groupMapper.selectList(qw);

        Map<Integer, List<Integer>> parentChildMap = groups.stream()
                .filter(g -> g.getParentId() != null)
                .collect(Collectors.groupingBy(RuleGroup::getParentId,
                        Collectors.mapping(RuleGroup::getId, Collectors.toList())));

        Set<Integer> result = new LinkedHashSet<>();
        collectChildren(id, parentChildMap, result);
        return new ArrayList<>(result);
    }

    /**
     * 递归收集所有子分类ID
     *
     * @param parentId       父级分类ID
     * @param parentChildMap 父子分类映射
     * @param out            输出结果
     */
    private void collectChildren(Integer parentId, Map<Integer, List<Integer>> parentChildMap, Set<Integer> out) {
        List<Integer> children = parentChildMap.get(parentId);
        if (children == null || children.isEmpty()) {
            return;
        }
        for (Integer child : children) {
            if (out.add(child)) {
                collectChildren(child, parentChildMap, out);
            }
        }
    }

    /**
     * 判断分类ID是否为固定分类ID
     *
     * @param groupId 分类ID
     * @return 是否为固定分类ID
     */
    private boolean isFixedGroupId(Integer groupId) {
        if (groupId == null) {
            return false;
        }
        String idStr = String.valueOf(groupId);
        for (String fixed : getRegularGroupId()) {
            if (idStr.equals(fixed)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 验证分类创建权限
     *
     * @param parentId 父级分类ID
     */
    private void validateGroupCreatePermission(Integer parentId) {
        if (parentId == null) {
            throw new BusinessException("请选择公共或自定义目录作为父级！");
        }
        Integer firstGroupId = getFirstGroupId(parentId);
        Integer customId = resolveCustomGroupId();
        Integer publicId = resolvePublicGroupId();
        if (customId == null || publicId == null) {
            throw new BusinessException("规则分类顶级目录（公共/自定义）未配置完整！");
        }
        if (isAdmin()) {
            if (!Objects.equals(firstGroupId, customId) && !Objects.equals(firstGroupId, publicId)) {
                throw new BusinessException("管理员仅可在公共或自定义目录下新建分组！");
            }
            return;
        }
        if (!Objects.equals(firstGroupId, customId)) {
            throw new BusinessException("非管理员仅可在自定义目录下新建分组！");
        }
    }

    /**
     * 判断当前用户是否为管理员
     */
    private boolean isAdmin() {
        Integer uid = UserContext.getCurrentUserId();
        if (uid == null) {
            return false;
        }
        List<RoleVo> roles = roleService.getRoleListByUserId(uid);
        return roles.stream().anyMatch(r -> Constants.ROLE_NAME.ADMIN.equals(r.getRoleName()));
    }

    /**
     * 解析公共分类ID
     */
    private Integer resolvePublicGroupId() {
        Integer fromCfg = parseIntOrNull(publicGroupIdCfg);
        if (fromCfg != null) {
            return fromCfg;
        }
        return findTopGroupIdByKeyword("公共");
    }

    /**
     * 解析自定义分类ID
     */
    private Integer resolveCustomGroupId() {
        Integer fromCfg = parseIntOrNull(customGroupIdCfg);
        if (fromCfg != null) {
            return fromCfg;
        }
        return findTopGroupIdByKeyword("自定义");
    }

    /**
     * 查找顶级分类ID
     */
    private Integer findTopGroupIdByKeyword(String keyword) {
        LambdaQueryWrapper<RuleGroup> qw = new LambdaQueryWrapper<>();
        qw.eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE)
                .isNull(RuleGroup::getParentId);
        List<RuleGroup> roots = groupMapper.selectList(qw);
        for (RuleGroup root : roots) {
            if (root.getGroupName() != null && root.getGroupName().contains(keyword)) {
                return root.getId();
            }
        }
        return null;
    }

    /**
     * 尝试解析整数，若失败则返回null
     */
    private Integer parseIntOrNull(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
