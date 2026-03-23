package com.pufferfishscheduler.worker.task.metadata.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.mapper.DbDatabaseMapper;
import com.pufferfishscheduler.worker.task.metadata.service.DbDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author Mayc
 * @since 2026-03-22  12:00
 */
@Component
public class DbDatabaseServiceImpl implements DbDatabaseService {

    @Autowired
    private DbDatabaseMapper dbDatabaseMapper;

    /**
     * 根据ID查询数据库
     *
     * @param dbId 数据库ID
     * @return 数据库
     */
    @Override
    public DbDatabase getDatabaseById(Integer dbId) {
        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(DbDatabase::getId, dbId)
                .eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);
        return dbDatabaseMapper.selectOne(queryWrapper);
    }
}
