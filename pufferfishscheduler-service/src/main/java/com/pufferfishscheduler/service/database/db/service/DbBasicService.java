package com.pufferfishscheduler.service.database.db.service;

import com.pufferfishscheduler.domain.vo.database.DbBasicVo;
import com.pufferfishscheduler.domain.vo.database.DbCategoryVo;

import java.util.List;

/**
 * (DbBasic)表服务接口
 *
 * @author mayc
 * @since 2025-05-20 23:34:43
 */
public interface DbBasicService {

    /**
     * query database category list
     *
     * @return
     */
    List<DbCategoryVo> getDbCategoryList();

    /**
     * query database basic list
     *
     * @return
     */
    List<DbBasicVo> getDbBasicList();

    /**
     * query database basic list by category id
     *
     * @param categoryId
     * @return
     */
    List<DbBasicVo> getDbBasicListByCategoryId(Integer categoryId);

    /**
     * 获取数据源图标
     * @param name
     * @return
     */
    String getDbIcon(String name);
}

