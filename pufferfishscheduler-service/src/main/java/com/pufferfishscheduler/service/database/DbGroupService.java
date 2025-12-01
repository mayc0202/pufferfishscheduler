package com.pufferfishscheduler.service.database;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.domain.form.database.DbGroupForm;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.dao.entity.DbGroup;
import com.pufferfishscheduler.domain.vo.dict.Dict;
import com.pufferfishscheduler.domain.vo.dict.DictItem;

import java.util.List;

/**
 * (DbGroup) Service
 *
 * @author makejava
 * @since 2025-05-22 00:00:40
 */
public interface DbGroupService extends IService<DbGroup> {

    /**
     * 分组字典
     *
     * @return
     */
    List<DictItem> dict();

    /**
     * 获取数据源树形结构
     *
     * @param name
     * @return
     */
    List<Tree> tree(String name);

    /**
     * 获取FTP数据源树形结构
     *
     * @return
     */
    List<Tree> ftpDbTree(String name);

    /**
     * 获取关系型数据源树形结构
     *
     * @return
     */
    List<Tree> relationalDbTree();


    /**
     * 获取分组集合
     *
     * @return
     */
    List<DbGroup> getGroupList(String name);

    /**
     * 添加分组
     *
     * @param form
     */
    void add(DbGroupForm form);

    /**
     * 编辑分组
     *
     * @param form
     */
    void update(DbGroupForm form);

    /**
     * 删除分组
     *
     * @param id
     */
    void delete(Integer id);
}

