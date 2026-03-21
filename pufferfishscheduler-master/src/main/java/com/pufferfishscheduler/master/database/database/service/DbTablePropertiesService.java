package com.pufferfishscheduler.master.database.database.service;

import com.pufferfishscheduler.dao.entity.DbTableProperties;

import java.util.List;

/**
 * 数据库表属性Service接口
 *
 * @author Mayc
 * @since 2026-03-17  18:25
 */
public interface DbTablePropertiesService {

    /**
     * 根据数据库ID删除所有表的属性配置
     */
    void deleteByDatabaseId(Integer databaseId);

    /**
     * 批量插入表属性配置
     */
    void batchInsert(List<DbTableProperties> list);

    /**
     * 根据表ID和属性类型查询属性值
     */
    String selectTableProperties(Integer tableId, String type);
}
