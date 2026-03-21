package com.pufferfishscheduler.master.database.database.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.DbTableProperties;
import com.pufferfishscheduler.dao.mapper.DbTablePropertiesMapper;
import com.pufferfishscheduler.domain.model.database.DatabaseTable;
import com.pufferfishscheduler.master.database.database.service.DbTablePropertiesService;
import com.pufferfishscheduler.master.database.database.service.DbTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 数据库表属性Service实现类
 *
 * @author Mayc
 * @since 2026-03-17  18:25
 */
@Service
public class DbTablePropertiesServiceImpl implements DbTablePropertiesService {

    @Autowired
    private DbTablePropertiesMapper dbTablePropertiesMapper;

    @Autowired
    private DbTableService dbTableService;

    /**
     * 根据数据库ID删除所有表的属性配置
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void deleteByDatabaseId(Integer databaseId) {
        LambdaQueryWrapper<DbTableProperties> example = new LambdaQueryWrapper<>();
        example.eq(DbTableProperties::getPropType, Constants.TABLE_PROPERTIES.PRIMARY_KEY)
                .in(DbTableProperties::getTableId, getTableIdsByDatabaseId(databaseId));
        dbTablePropertiesMapper.delete(example);
    }

    /**
     * 批量插入数据库表属性
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void batchInsert(List<DbTableProperties> list) {
        dbTablePropertiesMapper.batchInsert(list);
    }

    /**
     * 根据表ID和属性类型查询属性值
     */
    @Override
    public String selectTableProperties(Integer tableId, String type) {
        LambdaQueryWrapper<DbTableProperties> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(DbTableProperties::getPropType, type)
                .eq(DbTableProperties::getTableId, tableId);
        DbTableProperties dbTablePropertiesList = dbTablePropertiesMapper.selectOne(queryWrapper);
        if (dbTablePropertiesList == null) {
            return null;
        }
        return dbTablePropertiesList.getPropValues();
    }

    /**
     * 根据数据库ID获取所有表ID
     * 需要根据您的实际情况实现
     */
    private List<Integer> getTableIdsByDatabaseId(Integer databaseId) {
        return dbTableService.getTablesByDbId(databaseId).stream()
                .map(DatabaseTable::getId)
                .collect(Collectors.toList());
    }
}
