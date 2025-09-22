package com.pufferfishscheduler.service.database.db.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.domain.form.database.DbGroupForm;
import com.pufferfishscheduler.domain.vo.TreeVo;
import com.pufferfishscheduler.dao.entity.DbGroup;

import java.util.List;

/**
 * (DbGroup) Service
 *
 * @author makejava
 * @since 2025-05-22 00:00:40
 */
public interface DbGroupService extends IService<DbGroup> {

    /**
     * 获取分组树形结构
     *
     * @param name
     * @return
     */
    List<TreeVo> tree(String name);


    /**
     * 获取分组集合
     *
     * @return
     */
    List<DbGroup> getGroupList();

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

