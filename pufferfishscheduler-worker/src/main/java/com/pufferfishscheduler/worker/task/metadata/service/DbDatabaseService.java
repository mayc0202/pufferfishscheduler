package com.pufferfishscheduler.worker.task.metadata.service;

import com.pufferfishscheduler.dao.entity.DbDatabase;

/**
 * 数据库元数据服务接口
 *
 * @author Mayc
 * @since 2026-03-22  19:31
 */
public interface DbDatabaseService {

    /**
     * 根据ID查询数据库
     *
     * @param dbId 数据库ID
     * @return 数据库
     */
     DbDatabase getDatabaseById(Integer dbId);
}
