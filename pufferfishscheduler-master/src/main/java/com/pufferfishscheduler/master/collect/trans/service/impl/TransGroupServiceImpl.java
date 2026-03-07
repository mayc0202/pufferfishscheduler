package com.pufferfishscheduler.master.collect.trans.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.node.GenericTreeBuilder;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.common.node.TreeNode;
import com.pufferfishscheduler.dao.entity.TransGroup;
import com.pufferfishscheduler.dao.mapper.TransGroupMapper;
import com.pufferfishscheduler.domain.form.collect.TransGroupForm;
import com.pufferfishscheduler.domain.vo.dict.DictItem;
import com.pufferfishscheduler.master.collect.trans.service.TransGroupService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 转换分组服务实现类
 *
 * @author Mayc
 * @since 2026-03-04  13:36
 */
@Slf4j
@Service("transGroupService")
public class TransGroupServiceImpl extends ServiceImpl<TransGroupMapper, TransGroup> implements TransGroupService {

    @Autowired
    private TransGroupMapper transGroupMapper;

    @Autowired
    private GenericTreeBuilder treeBuilder;

    /**
     * 封装转换分组字典
     *
     * @return
     */
    @Override
    public List<DictItem> dict() {
        List<TransGroup> groupList = getGroupList(null);
        if (CollectionUtils.isEmpty(groupList)) {
            return Collections.emptyList();
        }

        List<DictItem> itemList = new LinkedList<>();
        for (TransGroup group : groupList) {
            DictItem dictItem = new DictItem();
            dictItem.setCode(group.getName());
            dictItem.setValue(group.getId().toString());
            dictItem.setOrder(group.getOrderBy());
            itemList.add(dictItem);
        }

        return itemList;
    }

    /**
     * 获取转换分组树形结构
     *
     * @param name
     * @return
     */
    @Override
    public List<Tree> tree(String name) {
        List<TransGroup> groups = getGroupList(name);

        // 转换为Tree节点
        List<Tree> allNodes = convertToTreeNodes(groups);

        // 构建树
        List<Tree> tree = treeBuilder.buildTree(allNodes,
                Comparator.comparing(TreeNode::getOrder));

        return tree;
    }

    /**
     * 节点转换
     *
     * @param groups
     * @return
     */
    public List<Tree> convertToTreeNodes(List<TransGroup> groups) {
        List<Tree> nodes = new ArrayList<>();

        // 转换分组
        for (TransGroup group : groups) {
            Tree tree = new Tree();
            BeanUtils.copyProperties(group, tree);
            tree.setType(Constants.TREE_TYPE.GROUP);
            tree.setOrderBy(group.getOrderBy());
            nodes.add(tree);
        }

        return nodes;
    }

    /**
     * 获取分组列表
     *
     * @return
     */
    @Override
    public List<TransGroup> getGroupList(String name) {
        LambdaQueryWrapper<TransGroup> ldq = new LambdaQueryWrapper<>();
        ldq.eq(TransGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        ldq.orderByDesc(TransGroup::getOrderBy);

        if (StringUtils.isNotBlank(name)) {
            ldq.like(TransGroup::getName, name);
        }

        List<TransGroup> transGroups = transGroupMapper.selectList(ldq);
        if (CollectionUtils.isEmpty(transGroups)) {
            return Collections.emptyList();
        }
        return transGroups;
    }

    /**
     * 添加分组
     *
     * @param form
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void add(TransGroupForm form) {

        LambdaQueryWrapper<TransGroup> ldq = new LambdaQueryWrapper<>();
        ldq.eq(TransGroup::getName, form.getName())
                .eq(TransGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        TransGroup transGroup = transGroupMapper.selectOne(ldq);
        if (Objects.nonNull(transGroup)) {
            throw new BusinessException(String.format("分组名称【%s】已存在!", form.getName()));
        }

        TransGroup group = new TransGroup();
        group.setName(form.getName());
        group.setOrderBy(form.getOrderBy());
        group.setParentId(form.getParentId());
        group.setDeleted(Constants.DELETE_FLAG.FALSE);
        group.setCreatedBy(UserContext.getCurrentAccount());
        group.setCreatedTime(new Date());
        transGroupMapper.insert(group);
    }

    /**
     * 编辑分组
     *
     * @param form
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void update(TransGroupForm form) {

        LambdaQueryWrapper<TransGroup> ldq1 = new LambdaQueryWrapper<>();
        ldq1.eq(TransGroup::getId, form.getId())
                .eq(TransGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        TransGroup group = transGroupMapper.selectOne(ldq1);
        if (Objects.isNull(group)) {
            throw new BusinessException("请校验分组是否存在!");
        }

        // 校验分组名称是否已存在
        verifyGroupIsExists(form.getName(), form.getId());

        group.setName(form.getName());
        group.setOrderBy(form.getOrderBy());
        group.setParentId(form.getParentId());
        group.setUpdatedBy(UserContext.getCurrentAccount());
        group.setUpdatedTime(new Date());
        transGroupMapper.updateById(group);
    }

    /**
     * 删除分组
     *
     * @param id
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void delete(Integer id) {
        
        // 校验分组是否存在
        verifyGroupExist(id);

        // 校验分组下是否存在子分组
        verifyGroupHasChildren(id);

        // 使用 UpdateWrapper 进行更新
        UpdateWrapper<TransGroup> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", id)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());

        transGroupMapper.update(null, updateWrapper);
    }

    /**
     * 校验分组是否存在
     *
     * @param id
     * @return
     */
    private void verifyGroupExist(Integer id) {
        // 查询转换流
        LambdaQueryWrapper<TransGroup> ldq = new LambdaQueryWrapper<>();
        ldq.eq(TransGroup::getId, id)
                .eq(TransGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        TransGroup transGroup = transGroupMapper.selectOne(ldq);
        if (Objects.isNull(transGroup)) {
            throw new BusinessException("转换分组不存在!");
        }
    }

    /**
     * 校验分组下是否存在子分组
     *
     * @param groupId
     */
    private void verifyGroupHasChildren(Integer groupId) {
        LambdaQueryWrapper<TransGroup> ldq = new LambdaQueryWrapper<>();
        ldq.eq(TransGroup::getParentId, groupId)
                .eq(TransGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        List<TransGroup> children = transGroupMapper.selectList(ldq);
        if (!children.isEmpty()) {
            throw new BusinessException("当前分组下存在子分组!");
        }
    }

    /**
     * 校验分组是否存在
     *
     * @param name
     * @param excludeId
     */
    private void verifyGroupIsExists(String name, Integer excludeId) {
        LambdaQueryWrapper<TransGroup> ldq = new LambdaQueryWrapper<>();
        ldq.eq(TransGroup::getName, name)
                .eq(TransGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        if (excludeId != null) {
            ldq.ne(TransGroup::getId, excludeId);
        }

        TransGroup group = transGroupMapper.selectOne(ldq);
        if (Objects.nonNull(group)) {
            throw new BusinessException(String.format("分组名称【%s】已存在!", name));
        }
    }

    /**
     * 根据分组ID获取分组的全路径名称（用/分割）
     * @param groupId 分组ID
     * @return 全路径名称，例如：顶级分组/二级分组/三级分组
     */
    @Override
    public String getGroupFullPath(Integer groupId) {
        if (groupId == null) {
            return "";
        }

        // 缓存所有分组，避免多次查询数据库
        Map<Integer, TransGroup> allGroupMap = getAllGroupMap();

        // 存储路径的临时列表（从子到父）
        List<String> pathNames = new ArrayList<>();

        // 递归向上查找父级
        Integer currentId = groupId;
        while (currentId != null) {
            TransGroup group = allGroupMap.get(currentId);
            if (group == null) {
                break; // 找不到当前分组，终止
            }

            pathNames.add(group.getName());
            currentId = group.getParentId(); // 获取父级ID
        }

        // 反转列表（从父到子）并拼接
        Collections.reverse(pathNames);
        return String.join("/", pathNames);
    }

    /**
     * 获取所有有效分组的ID-对象映射
     * @return 分组ID为key，分组对象为value
     */
    private Map<Integer, TransGroup> getAllGroupMap() {
        List<TransGroup> allGroups = getGroupList(null);
        return allGroups.stream()
                .collect(Collectors.toMap(TransGroup::getId, group -> group));
    }

    /**
     * 批量获取分组ID对应的全路径名称
     * @param groupIds 分组ID列表
     * @return 分组ID为key，全路径名称为value
     */
    @Override
    public Map<Integer, String> getGroupFullPathMap(Collection<Integer> groupIds) {
        if (CollectionUtils.isEmpty(groupIds)) {
            return Collections.emptyMap();
        }

        Map<Integer, TransGroup> allGroupMap = getAllGroupMap();
        Map<Integer, String> pathMap = new HashMap<>();

        for (Integer groupId : groupIds) {
            pathMap.put(groupId, getGroupFullPath(groupId, allGroupMap));
        }

        return pathMap;
    }

    /**
     * 重载方法：使用已有的分组映射获取全路径，避免重复构建map
     * @param groupId 分组ID
     * @param allGroupMap 所有分组的映射
     * @return 全路径名称
     */
    private String getGroupFullPath(Integer groupId, Map<Integer, TransGroup> allGroupMap) {
        if (groupId == null || allGroupMap.isEmpty()) {
            return "";
        }

        List<String> pathNames = new ArrayList<>();
        Integer currentId = groupId;

        while (currentId != null) {
            TransGroup group = allGroupMap.get(currentId);
            if (group == null) {
                break;
            }

            pathNames.add(group.getName());
            currentId = group.getParentId();
        }

        Collections.reverse(pathNames);
        return String.join("/", pathNames);
    }

    /**
     * 根据父分组ID，递归获取所有子分组ID（包含自身）
     * @param parentGroupId 父分组ID
     * @return 所有子分组ID列表（包含父ID）
     */
    @Override
    public List<Integer> getAllChildGroupIds(Integer parentGroupId) {
        if (parentGroupId == null) {
            return Collections.emptyList();
        }

        // 获取所有分组映射
        Map<Integer, TransGroup> allGroupMap = getAllGroupMap();

        // 存储所有子分组ID（包含自身）
        List<Integer> allChildIds = new ArrayList<>();
        // 先添加父ID
        allChildIds.add(parentGroupId);

        // 递归查找所有子分组
        findAllChildGroups(parentGroupId, allGroupMap, allChildIds);

        return allChildIds;
    }

    /**
     * 递归查找所有子分组ID
     * @param parentId 父分组ID
     * @param allGroupMap 所有分组映射
     * @param result 结果列表
     */
    private void findAllChildGroups(Integer parentId, Map<Integer, TransGroup> allGroupMap, List<Integer> result) {
        // 遍历所有分组，找到父ID匹配的子分组
        for (Map.Entry<Integer, TransGroup> entry : allGroupMap.entrySet()) {
            TransGroup group = entry.getValue();
            if (parentId.equals(group.getParentId())) {
                // 添加子分组ID
                result.add(group.getId());
                // 递归查找该子分组的子分组
                findAllChildGroups(group.getId(), allGroupMap, result);
            }
        }
    }
}
