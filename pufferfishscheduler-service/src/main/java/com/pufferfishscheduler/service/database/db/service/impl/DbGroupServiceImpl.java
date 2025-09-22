package com.pufferfishscheduler.service.database.db.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.form.database.DbGroupForm;
import com.pufferfishscheduler.domain.vo.TreeVo;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.DbGroup;
import com.pufferfishscheduler.dao.mapper.DbDatabaseMapper;
import com.pufferfishscheduler.dao.mapper.DbGroupMapper;
import com.pufferfishscheduler.service.database.db.service.DbBasicService;
import com.pufferfishscheduler.service.database.db.service.DbDatabaseService;
import com.pufferfishscheduler.service.database.db.service.DbGroupService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.function.Function;
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

    /**
     * 获取分组树形结构
     *
     * @param name
     * @return
     */
    @Override
    public List<TreeVo> tree(String name) {

        // 获取所有分组
        List<DbGroup> dbGroups = getGroupList(name);
        if (dbGroups.isEmpty()) {
            return Collections.emptyList();
        }

        Set<Integer> groupIds = dbGroups.stream()
                .map(DbGroup::getId)
                .collect(Collectors.toSet());

        // 获取数据源集合
        List<DbDatabase> databaseList = databaseService.listDatabasesByGroupIds(groupIds);
        if (CollectionUtils.isEmpty(databaseList)) {
            return Collections.emptyList();
        }

        // key=groupId,value=List<DbDatabase>
        Map<Integer, List<DbDatabase>> groupDatabaseMap = databaseList.stream()
                .collect(Collectors.groupingBy(
                        DbDatabase::getGroupId,
                        Collectors.mapping(Function.identity(), Collectors.toList())
                ));


        return dbGroups.stream().map(group -> {
            TreeVo vo = new TreeVo();
            BeanUtils.copyProperties(group, vo);
            vo.setType(Constants.TREE_TYPE.GROUP);

            List<TreeVo> children = Optional.ofNullable(groupDatabaseMap.get(group.getId()))
                    .orElse(Collections.emptyList())
                    .stream()
                    .map(db -> {
                        TreeVo child = new TreeVo();
                        child.setId(db.getId());
                        child.setIcon(dbBasicService.getDbIcon(db.getType())); // deal icon
                        child.setName(db.getName());
                        child.setType(Constants.TREE_TYPE.DATABASE);
                        return child;
                    })
                    .collect(Collectors.toList());

            vo.setChildren(children);
            return vo;
        }).collect(Collectors.toList());
    }

    /**
     * 获取分组列表
     *
     * @return
     */
    @Override
    public List<DbGroup> getGroupList() {
        LambdaQueryWrapper<DbGroup> groupLdq = new LambdaQueryWrapper<>();
        groupLdq.eq(DbGroup::getDeleted, Constants.DELETE_FLAG.FALSE);
        groupLdq.orderByDesc(DbGroup::getOrderBy);
        List<DbGroup> dbGroups = dbGroupDao.selectList(groupLdq);
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

        group.setDeleted(Constants.DELETE_FLAG.TRUE);
        group.setUpdatedBy(UserContext.getCurrentAccount());
        group.setUpdatedTime(new Date());
        dbGroupDao.updateById(group);
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

    /**
     * 获取所有数据源分组
     *
     * @param name
     * @return
     */
    private List<DbGroup> getGroupList(String name) {

        LambdaQueryWrapper<DbGroup> ldq = new LambdaQueryWrapper<>();
        ldq.eq(DbGroup::getDeleted, Constants.DELETE_FLAG.FALSE)
                .orderByAsc(DbGroup::getOrderBy);

        if (StringUtils.isNotBlank(name)) {
            ldq.like(DbGroup::getName, name);
        }

        return dbGroupDao.selectList(ldq);
    }


}

