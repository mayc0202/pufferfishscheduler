package com.pufferfishscheduler.worker.task.metadata.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.CommonUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.DbField;
import com.pufferfishscheduler.dao.entity.DbTable;
import com.pufferfishscheduler.dao.entity.DbTableProperties;
import com.pufferfishscheduler.dao.mapper.DbDatabaseMapper;
import com.pufferfishscheduler.dao.mapper.DbFieldMapper;
import com.pufferfishscheduler.dao.mapper.DbTableMapper;
import com.pufferfishscheduler.dao.mapper.DbTablePropertiesMapper;
import com.pufferfishscheduler.domain.domain.TableColumnSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.domain.model.database.DBConnectionInfo;
import com.pufferfishscheduler.worker.task.connect.relationdb.AbstractDatabaseConnector;
import com.pufferfishscheduler.worker.task.connect.relationdb.DatabaseConnectorFactory;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Worker 执行器：数据库元数据同步（由 Master 派发 invokeTarget 调用）。
 */
@Slf4j
@Component("dbSyncExecutor")
@AllArgsConstructor
public class DbSyncExecutor {

    private final DbDatabaseMapper dbDatabaseMapper;
    private final DbTableMapper dbTableMapper;
    private final DbFieldMapper dbFieldMapper;
    private final DbTablePropertiesMapper dbTablePropertiesMapper;
    private final AESUtil aesUtil;

    /**
     * 执行数据库元数据同步
     *
     * @param databaseId 数据库id
     */
    public void execute(int databaseId) {
        syncTableInfo(databaseId);
    }

    /**
     * 同步数据库表信息到数据库元数据表
     *
     * @param databaseId 数据库id
     */
    @Transactional(rollbackFor = Exception.class)
    public void syncTableInfo(int databaseId) {
        log.info("开始同步数据库表信息, databaseId={}", databaseId);
        try {
            DbDatabase dbDatabase = getDatabaseById(databaseId);
            DBConnectionInfo connInfo = buildConnectionInfo(dbDatabase);
            AbstractDatabaseConnector connector = buildConnector(connInfo);
            Map<String, TableSchema> tableInfoMap = connector.getTableSchema(null, null);
            if (tableInfoMap == null || tableInfoMap.isEmpty()) {
                return;
            }
            String currentUser = Optional.ofNullable(UserContext.getCurrentAccount()).orElse(Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);

            syncTablesToDatabase(databaseId, tableInfoMap, currentUser);

            List<DbTableProperties> primaryKeyList = new ArrayList<>();
            syncFieldsToDatabase(databaseId, tableInfoMap, currentUser, primaryKeyList);
            syncPrimaryKeysToDatabase(databaseId, primaryKeyList, currentUser);
        } finally {
            log.info("同步数据库表信息完成, databaseId={}", databaseId);
        }
    }

    /**
     *
     * 根据 ID 获取数据库元数据
     *
     * @param id 数据库id
     * @return 数据库元数据
     */
    private DbDatabase getDatabaseById(int id) {
        LambdaQueryWrapper<DbDatabase> query = new LambdaQueryWrapper<>();
        query.eq(DbDatabase::getId, id).eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);
        DbDatabase database = dbDatabaseMapper.selectOne(query);
        if (database == null) {
            log.error("数据源不存在, id={}", id);
            throw new BusinessException(String.format("数据源不存在, id=%s", id));
        }
        return database;
    }

    /**
     * 构建数据库连接信息
     *
     * @param dbDatabase 数据库元数据
     * @return 数据库连接信息
     */
    private DBConnectionInfo buildConnectionInfo(DbDatabase dbDatabase) {
        DBConnectionInfo info = new DBConnectionInfo();
        BeanUtils.copyProperties(dbDatabase, info);
        info.setPassword(aesUtil.decrypt(dbDatabase.getPassword()));
        return info;
    }

    /**
     * 构建数据库连接
     *
     * @param info 数据库连接信息
     * @return 数据库连接
     */
    private AbstractDatabaseConnector buildConnector(DBConnectionInfo info) {
        AbstractDatabaseConnector connector = DatabaseConnectorFactory.getConnector(info.getType());
        connector.setDbName(info.getDbName());
        connector.setUsername(info.getUsername());
        connector.setPassword(info.getPassword());
        connector.setHost(info.getDbHost());
        if (StringUtils.isNotBlank(info.getDbPort())) {
            connector.setPort(Integer.parseInt(info.getDbPort()));
        }
        connector.setSchema(info.getDbSchema());
        connector.setProperties(info.getProperties());
        connector.setExtConfig(info.getExtConfig());
        connector.setType(info.getType());
        connector.build();
        return connector;
    }

    /**
     * 同步数据库表信息
     *
     * @param databaseId   数据库id
     * @param tableInfoMap 表信息映射表
     * @param currentUser  当前用户
     */
    private void syncTablesToDatabase(int databaseId, Map<String, TableSchema> tableInfoMap, String currentUser) {
        LambdaQueryWrapper<DbTable> tableQuery = new LambdaQueryWrapper<>();
        tableQuery.eq(DbTable::getDbId, databaseId).eq(DbTable::getDeleted, Constants.DELETE_FLAG.FALSE);
        List<DbTable> existingTables = dbTableMapper.selectList(tableQuery);
        Map<String, DbTable> existingTableMap = existingTables.stream()
                .collect(Collectors.toMap(DbTable::getName, t -> t, (a, b) -> a));

        Date now = new Date();
        for (TableSchema source : tableInfoMap.values()) {
            DbTable target = existingTableMap.get(source.getTableName());
            if (target == null) {
                target = new DbTable();
                target.setDbId(databaseId);
                target.setName(source.getTableName());
                target.setBusinessName(source.getTableName());
                target.setDescription(source.getTableComment());
                target.setDeleted(Constants.DELETE_FLAG.FALSE);
                target.setCreatedBy(currentUser);
                target.setCreatedTime(now);
                target.setUpdatedBy(currentUser);
                target.setUpdatedTime(now);
                dbTableMapper.insert(target);
            } else {
                target.setDescription(source.getTableComment());
                target.setUpdatedBy(currentUser);
                target.setUpdatedTime(now);
                dbTableMapper.updateById(target);
            }
        }

        Set<String> currentTableNames = tableInfoMap.keySet();
        for (DbTable existing : existingTables) {
            if (!currentTableNames.contains(existing.getName())) {
                existing.setDeleted(Constants.DELETE_FLAG.TRUE);
                existing.setUpdatedBy(currentUser);
                existing.setUpdatedTime(now);
                dbTableMapper.updateById(existing);
            }
        }
    }

    /**
     * 同步数据库字段信息
     *
     * @param databaseId     数据库id
     * @param tableInfoMap   表信息映射表
     * @param currentUser    当前用户
     * @param primaryKeyList 主键列表
     */
    private void syncFieldsToDatabase(int databaseId, Map<String, TableSchema> tableInfoMap,
                                      String currentUser, List<DbTableProperties> primaryKeyList) {
        LambdaQueryWrapper<DbTable> tableQuery = new LambdaQueryWrapper<>();
        tableQuery.eq(DbTable::getDbId, databaseId).eq(DbTable::getDeleted, Constants.DELETE_FLAG.FALSE);
        Map<String, Integer> tableIdMap = dbTableMapper.selectList(tableQuery).stream()
                .collect(Collectors.toMap(DbTable::getName, DbTable::getId, (a, b) -> a));

        Date now = new Date();
        for (TableSchema sourceTable : tableInfoMap.values()) {
            Integer tableId = tableIdMap.get(sourceTable.getTableName());
            if (tableId == null) continue;

            LambdaQueryWrapper<DbField> fieldQuery = new LambdaQueryWrapper<>();
            fieldQuery.eq(DbField::getTableId, tableId).eq(DbField::getDeleted, Constants.DELETE_FLAG.FALSE);
            List<DbField> existingFields = dbFieldMapper.selectList(fieldQuery);
            Map<String, DbField> existingFieldMap = existingFields.stream()
                    .collect(Collectors.toMap(DbField::getName, f -> f, (a, b) -> a));

            Map<String, TableColumnSchema> columnInfoMap = sourceTable.getColumnInfos();
            if (columnInfoMap != null) {
                for (TableColumnSchema col : columnInfoMap.values()) {
                    DbField f = existingFieldMap.get(col.getColumnName());
                    if (f == null) {
                        f = new DbField();
                        f.setTableId(tableId);
                        f.setName(col.getColumnName());
                        f.setBusinessName(col.getColumnName());
                        f.setDataType(col.getDataType());
                        f.setDataLength(col.getDataLength());
                        f.setDescription(col.getColumnComment());
                        f.setNullable(col.getIsNull());
                        f.setOrderBy(col.getColumnOrder());
                        f.setPrecision(col.getPrecision());
                        f.setDeleted(Constants.DELETE_FLAG.FALSE);
                        f.setCreatedBy(currentUser);
                        f.setCreatedTime(now);
                        f.setUpdatedBy(currentUser);
                        f.setUpdatedTime(now);
                        dbFieldMapper.insert(f);
                    } else {
                        f.setDataType(col.getDataType());
                        f.setDataLength(col.getDataLength());
                        f.setDescription(col.getColumnComment());
                        f.setNullable(col.getIsNull());
                        f.setOrderBy(col.getColumnOrder());
                        f.setPrecision(col.getPrecision());
                        f.setUpdatedBy(currentUser);
                        f.setUpdatedTime(now);
                        dbFieldMapper.updateById(f);
                    }
                    collectPrimaryKey(col, tableId, primaryKeyList);
                }
            }

            Set<String> currentFieldNames = columnInfoMap != null ? columnInfoMap.keySet() : Collections.emptySet();
            for (DbField existing : existingFields) {
                if (!currentFieldNames.contains(existing.getName())) {
                    existing.setDeleted(Constants.DELETE_FLAG.TRUE);
                    existing.setUpdatedBy(currentUser);
                    existing.setUpdatedTime(now);
                    dbFieldMapper.updateById(existing);
                }
            }
        }
    }

    /**
     * 收集主键信息
     *
     * @param columnSchema   字段信息
     * @param tableId        表id
     * @param primaryKeyList 主键列表
     */
    private void collectPrimaryKey(TableColumnSchema columnSchema, Integer tableId, List<DbTableProperties> primaryKeyList) {
        String constraintType = columnSchema.getConstraintType();
        boolean isPrimaryKey = "P".equalsIgnoreCase(constraintType) || "PRI".equalsIgnoreCase(constraintType);
        if (isPrimaryKey) {
            DbTableProperties p = new DbTableProperties();
            p.setId(CommonUtil.getUUIDString());
            p.setTableId(tableId);
            p.setPropType(Constants.TABLE_PROPERTIES.PRIMARY_KEY);
            p.setPropValues(columnSchema.getColumnName());
            primaryKeyList.add(p);
        }
    }

    /**
     * 同步数据库主键信息
     *
     * @param databaseId     数据库id
     * @param primaryKeyList 主键列表
     * @param currentUser    当前用户
     */
    private void syncPrimaryKeysToDatabase(int databaseId, List<DbTableProperties> primaryKeyList, String currentUser) {
        LambdaQueryWrapper<DbTable> tableQuery = new LambdaQueryWrapper<>();
        tableQuery.eq(DbTable::getDbId, databaseId).eq(DbTable::getDeleted, Constants.DELETE_FLAG.FALSE);
        List<Integer> tableIds = dbTableMapper.selectList(tableQuery).stream().map(DbTable::getId).collect(Collectors.toList());

        if (!tableIds.isEmpty()) {
            LambdaQueryWrapper<DbTableProperties> deleteQuery = new LambdaQueryWrapper<>();
            deleteQuery.eq(DbTableProperties::getPropType, Constants.TABLE_PROPERTIES.PRIMARY_KEY)
                    .in(DbTableProperties::getTableId, tableIds);
            dbTablePropertiesMapper.delete(deleteQuery);
        }

        if (primaryKeyList.isEmpty()) return;

        Map<Integer, List<DbTableProperties>> groupedByTableId = primaryKeyList.stream()
                .collect(Collectors.groupingBy(DbTableProperties::getTableId));
        List<DbTableProperties> finalList = new ArrayList<>();
        Date now = new Date();
        for (Map.Entry<Integer, List<DbTableProperties>> entry : groupedByTableId.entrySet()) {
            Integer tableId = entry.getKey();
            List<DbTableProperties> tablePks = entry.getValue();
            if (tablePks.size() > 1) {
                DbTableProperties combined = new DbTableProperties();
                combined.setId(CommonUtil.getUUIDString());
                combined.setTableId(tableId);
                combined.setPropType(Constants.TABLE_PROPERTIES.PRIMARY_KEY);
                combined.setPropValues(tablePks.stream().map(DbTableProperties::getPropValues).collect(Collectors.joining(",")));
                combined.setCreatedBy(currentUser);
                combined.setCreatedTime(now);
                finalList.add(combined);
            } else {
                DbTableProperties single = tablePks.get(0);
                if (single.getId() == null || single.getId().isBlank()) {
                    single.setId(UUID.randomUUID().toString());
                }
                single.setCreatedBy(currentUser);
                single.setCreatedTime(now);
                finalList.add(single);
            }
        }

        if (!finalList.isEmpty()) {
            dbTablePropertiesMapper.batchInsert(finalList);
        }
    }
}

