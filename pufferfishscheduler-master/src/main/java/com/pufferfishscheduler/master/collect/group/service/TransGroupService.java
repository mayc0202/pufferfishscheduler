package com.pufferfishscheduler.master.collect.group.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.dao.entity.TransGroup;
import com.pufferfishscheduler.domain.form.collect.TransGroupForm;
import com.pufferfishscheduler.domain.vo.dict.DictItem;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Mayc
 * @since 2026-03-04  13:34
 */
public interface TransGroupService extends IService<TransGroup> {

    /**
     * 分组字典
     *
     * @return
     */
    List<DictItem> dict();

    /**
     * 获取转换分组树形结构
     *
     * @param name
     * @return
     */
    List<Tree> tree(String name);

    /**
     * 查询转换分组列表（树形结构，仅分组）
     *
     * @param name 分组名称
     * @return 仅分组树
     */
    List<Tree> groupTree(String name);

    /**
     * 获取分组集合
     *
     * @return
     */
    List<TransGroup> getGroupList(String name);

    /**
     * 添加分组
     *
     * @param form
     */
    void add(TransGroupForm form);

    /**
     * 编辑分组
     *
     * @param form
     */
    void update(TransGroupForm form);

    /**
     * 删除分组
     *
     * @param id
     */
    void delete(Integer id);

    /**
     * 获取分组全路径名称
     *
     * @param groupId 分组ID
     * @return 分组全路径名称
     */
    String getGroupFullPath(Integer groupId);

    /**
     * 获取分组ID集合对应的全路径名称映射
     *
     * @param groupIds 分组ID集合
     * @return 分组ID为key，全路径名称为value的映射
     */
    Map<Integer, String> getGroupFullPathMap(Collection<Integer> groupIds);
    /**
     * 获取所有子分组ID
     *
     * @param parentGroupId 父分组ID
     * @return 所有子分组ID
     */
    List<Integer> getAllChildGroupIds(Integer parentGroupId);
}
