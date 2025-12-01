package com.pufferfishscheduler.service.database.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.enums.DatabaseCategory;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.node.GenericTreeBuilder;
import com.pufferfishscheduler.common.node.TreeNode;
import com.pufferfishscheduler.domain.form.database.DbGroupForm;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.DbGroup;
import com.pufferfishscheduler.dao.mapper.DbGroupMapper;
import com.pufferfishscheduler.domain.vo.dict.Dict;
import com.pufferfishscheduler.domain.vo.dict.DictItem;
import com.pufferfishscheduler.service.database.DbBasicService;
import com.pufferfishscheduler.service.database.DbDatabaseService;
import com.pufferfishscheduler.service.database.DbGroupService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * (DbGroup) ServiceImpl
 *
 * @author mayc
 * @since 2025-05-22 00:00:40
 */
@Slf4j
@Service("dbGroupService")
public class DbGroupServiceImpl extends ServiceImpl<DbGroupMapper, DbGroup> implements DbGroupService {

    @Autowired
    private DbBasicService dbBasicService;

    @Autowired
    private DbDatabaseService databaseService;

    @Autowired
    private DbGroupMapper dbGroupDao;

    @Autowired
    private GenericTreeBuilder treeBuilder;

    /**
     * 封装数据源分组字典
     *
     * @return
     */
    @Override
    public List<DictItem> dict() {
        List<DbGroup> groupList = getGroupList(null);
        if (CollectionUtils.isEmpty(groupList)) {
            return Collections.emptyList();
        }

        List<DictItem> itemList = new LinkedList<>();
        for (DbGroup group : groupList) {
            DictItem dictItem = new DictItem();
            dictItem.setCode(group.getName());
            dictItem.setValue(group.getId().toString());
            dictItem.setOrder(group.getOrderBy());
            itemList.add(dictItem);
        }

        return itemList;
    }

    /**
     * 获取分组树形结构
     *
     * @param name
     * @return
     */
    @Override
    public List<Tree> tree(String name) {

        List<DbGroup> groups = getGroupList(name);
        List<DbDatabase> databases = getDatabasesByGroups(groups, null);

        // 转换为Tree节点
        List<Tree> allNodes = convertToTreeNodes(groups, databases, true);

        // 构建树
        List<Tree> tree = treeBuilder.buildTree(allNodes,
                Comparator.comparing(TreeNode::getOrder));

        // 过滤空分组
//        return treeBuilder.filterEmptyGroups(tree);
        return tree;
    }

    /**
     * 获取FTP数据源树形结构
     *
     * @return
     */
    @Override
    public List<Tree> ftpDbTree(String name) {
        List<DbGroup> groups = getGroupList(null);
        List<DbDatabase> databases = databaseService.listFTPDatabaseList(name);

        List<Tree> allNodes = convertToTreeNodes(groups, databases, true);
        List<Tree> tree = treeBuilder.buildTree(allNodes,
                Comparator.comparing(TreeNode::getOrder));

        return treeBuilder.filterEmptyGroups(tree);
    }

    /**
     * 获取关系型数据源树形结构
     *
     * @return
     */
    @Override
    public List<Tree> relationalDbTree() {
        List<DbGroup> groups = getGroupList(null);
        List<DbDatabase> databases = getDatabasesByGroups(groups, DatabaseCategory.SQL.getCode());

        // 过滤关系型数据库
        List<DbDatabase> relationalDbs = databases.stream()
                .filter(db -> DatabaseCategory.SQL.getCode().equals(db.getCategory()))
                .collect(Collectors.toList());

        List<Tree> allNodes = convertToTreeNodes(groups, relationalDbs, false);

        return treeBuilder.buildTree(allNodes, Comparator.comparing(TreeNode::getOrder));
    }

    /**
     * 节点转换
     *
     * @param groups
     * @param databases
     * @param showIcon
     * @return
     */
    public List<Tree> convertToTreeNodes(List<DbGroup> groups, List<DbDatabase> databases, Boolean showIcon) {
        List<Tree> nodes = new ArrayList<>();

        // 转换分组
        for (DbGroup group : groups) {
            Tree tree = new Tree();
            BeanUtils.copyProperties(group, tree);
            tree.setType(Constants.TREE_TYPE.GROUP);
            tree.setOrderBy(group.getOrderBy());
            nodes.add(tree);
        }

        // 转换数据库
        for (DbDatabase database : databases) {
            Tree tree = new Tree();
            tree.setId(database.getId());
            tree.setName(database.getName());
            if (showIcon) {
                tree.setIcon(dbBasicService.getDbIcon(database.getType())); // deal icon
            }
            tree.setType(Constants.TREE_TYPE.DATABASE);
            tree.setParentId(database.getGroupId());
            tree.setOrderBy(database.getId());
            // 设置其他属性...
            nodes.add(tree);
        }

        return nodes;
    }


    /**
     * 根据分组id获取数据源集合
     *
     * @param groups
     * @return
     */
    public List<DbDatabase> getDatabasesByGroups(List<DbGroup> groups, String category) {
        if (CollectionUtils.isEmpty(groups)) {
            return Collections.emptyList();
        }

        Set<Integer> groupIds = groups.stream()
                .map(DbGroup::getId)
                .collect(Collectors.toSet());

        return databaseService.listDatabasesByGroupIds(groupIds, category);
    }

    /**
     * 获取分组列表
     *
     * @return
     */
    @Override
    public List<DbGroup> getGroupList(String name) {
        LambdaQueryWrapper<DbGroup> ldq = new LambdaQueryWrapper<>();
        ldq.eq(DbGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        ldq.orderByDesc(DbGroup::getOrderBy);

        if (StringUtils.isNotBlank(name)) {
            ldq.like(DbGroup::getName, name);
        }

        List<DbGroup> dbGroups = dbGroupDao.selectList(ldq);
        if (CollectionUtils.isEmpty(dbGroups)) {
            return Collections.emptyList();
        }
        return dbGroups;
    }

    /**
     * Add group
     *
     * @param form
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void add(DbGroupForm form) {

        LambdaQueryWrapper<DbGroup> ldq = new LambdaQueryWrapper<>();
        ldq.eq(DbGroup::getName, form.getName())
                .eq(DbGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        DbGroup dbGroup = dbGroupDao.selectOne(ldq);
        if (Objects.nonNull(dbGroup)) {
            throw new BusinessException(String.format("分组名称【%s】已存在!", form.getName()));
        }

        DbGroup group = new DbGroup();
        group.setName(form.getName());
        group.setOrderBy(form.getOrderBy());
        group.setDeleted(Constants.DELETE_FLAG.FALSE);
        group.setCreatedBy(UserContext.getCurrentAccount());
        group.setCreatedTime(new Date());
        dbGroupDao.insert(group);
    }

    /**
     * 编辑分组
     *
     * @param form
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void update(DbGroupForm form) {

        LambdaQueryWrapper<DbGroup> ldq1 = new LambdaQueryWrapper<>();
        ldq1.eq(DbGroup::getId, form.getId())
                .eq(DbGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        DbGroup group = dbGroupDao.selectOne(ldq1);
        if (Objects.isNull(group)) {
            throw new BusinessException("请校验分组是否存在!");
        }

        // 校验分组名称是否已存在
        verifyGroupIsExists(form.getName());

        group.setName(form.getName());
        group.setOrderBy(form.getOrderBy());
        group.setUpdatedBy(UserContext.getCurrentAccount());
        group.setUpdatedTime(new Date());
        dbGroupDao.updateById(group);
    }

    /**
     * 删除分组
     *
     * @param id
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void delete(Integer id) {

        LambdaQueryWrapper<DbGroup> groupQueryWrapper = new LambdaQueryWrapper<>();
        groupQueryWrapper.eq(DbGroup::getId, id)
                .eq(DbGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        DbGroup group = dbGroupDao.selectOne(groupQueryWrapper);
        if (Objects.isNull(group)) {
            throw new BusinessException("请校验分组是否存在!");
        }

        // 校验分组下存在数据源不能删除
        verifyGroupHasDatabase(id);

        // 使用 UpdateWrapper 进行更新
        UpdateWrapper<DbGroup> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", id)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());

        dbGroupDao.update(null, updateWrapper);
    }

    /**
     * 校验分组下是否存在数据源
     *
     * @param groupId
     */
    private void verifyGroupHasDatabase(Integer groupId) {
        List<DbDatabase> list = databaseService.listDatabasesByGroupId(groupId);
        if (!list.isEmpty()) {
            throw new BusinessException("当前分组下存在数据源!");
        }
    }

    /**
     * 校验分组是否存在
     *
     * @param name
     */
    private void verifyGroupIsExists(String name) {
        LambdaQueryWrapper<DbGroup> ldq = new LambdaQueryWrapper<>();
        ldq.eq(DbGroup::getName, name)
                .eq(DbGroup::getDeleted, Constants.DELETE_FLAG.FALSE);

        DbGroup group = dbGroupDao.selectOne(ldq);
        if (Objects.nonNull(group)) {
            throw new BusinessException(String.format("分组名称【%s】已存在!", name));
        }
    }

}

