package com.pufferfishscheduler.service.database.db.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.model.DatabaseTable;
import com.pufferfishscheduler.dao.entity.DbTable;
import com.pufferfishscheduler.dao.mapper.DbTableMapper;
import com.pufferfishscheduler.service.database.db.service.DbDatabaseService;
import com.pufferfishscheduler.service.database.db.service.DbTableService;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author Mayc
 * @since 2025-09-07  01:03
 */
@Service
public class DbTableServiceImpl extends ServiceImpl<DbTableMapper, DbTable> implements DbTableService {

    @Autowired
    private DbTableMapper dbTableDao;


    @Autowired
    private DbDatabaseService databaseService;

    /**
     * 根据数据源id获取表信息集合
     *
     * @param dbId
     * @return
     */
    @Override
    public List<DatabaseTable> getTablesByDbId(Integer dbId) {
        // 验证数据源是否存在
        databaseService.validateDbExist(dbId);

        LambdaQueryWrapper<DbTable> lqd = new LambdaQueryWrapper<>();
        lqd.eq(DbTable::getDbId, dbId).eq(DbTable::getDeleted, Constants.DELETE_FLAG.FALSE);
        List<DbTable> dbTables = dbTableDao.selectList(lqd);
        if (dbTables.isEmpty()) {
            return Collections.emptyList();
        }

        return dbTables.stream()
                .map(this::buildDbTableInfo)
                .collect(Collectors.toList());
    }

    /**
     * 构建DbFieldInfo
     * @param dbTable
     * @return
     */
    private DatabaseTable buildDbTableInfo(DbTable dbTable) {
        DatabaseTable databaseTable = new DatabaseTable();
        BeanUtils.copyProperties(dbTable, databaseTable);
        return databaseTable;
    }

    /**
     * 校验表是否存在
     * @param tableId
     */
    @Override
    public void validateTableExist(Integer tableId) {
        LambdaQueryWrapper<DbTable> lqd = new LambdaQueryWrapper<>();
        lqd.eq(DbTable::getId, tableId).eq(DbTable::getDeleted, Constants.DELETE_FLAG.FALSE);
        DbTable dbTable = dbTableDao.selectOne(lqd);
        if (dbTable == null) {
            throw new BusinessException(String.format("请校验id[%s]表是否存在!",tableId));
        }
    }
}
