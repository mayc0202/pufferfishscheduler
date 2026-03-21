package com.pufferfishscheduler.master.database.metadata.service;

import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.utils.CommonUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.DbField;
import com.pufferfishscheduler.dao.entity.DbTable;
import com.pufferfishscheduler.dao.entity.DbTableProperties;
import com.pufferfishscheduler.domain.domain.TableColumnSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.domain.model.database.DatabaseTable;
import com.pufferfishscheduler.domain.model.database.DatabaseConnectionInfo;
import com.pufferfishscheduler.domain.model.database.DatabaseField;
import com.pufferfishscheduler.master.database.connect.relationdb.AbstractDatabaseConnector;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.master.database.database.service.DbFieldService;
import com.pufferfishscheduler.master.database.database.service.DbTablePropertiesService;
import com.pufferfishscheduler.master.database.database.service.DbTableService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 数据源同步执行器
 *
 * @author Mayc
 * @since 2025-08-15  00:29
 */
@Slf4j
@Component
public class DbSyncExecutor {

    @Autowired
    private DbDatabaseService dbDatabaseService;

    @Autowired
    private DbTableService dbTableService;

    @Autowired
    private DbFieldService dbFieldService;

    @Autowired
    private DbTablePropertiesService dbTablePropertiesService;

    /**
     * 同步执行，定时任务执行使用
     *
     * @param databaseId 数据库ID
     * @throws SQLException
     */
    public void execute(int databaseId) {
        DbSyncExecutor executor = PufferfishSchedulerApplicationContext.getBean(DbSyncExecutor.class);
        // 同步表基本信息
        executor.syncTableInfo(databaseId);
    }

    @Transactional
    public void syncTableInfo(int databaseId) {
        // 1.根据数据源ID获取数据源信息
        DbDatabase dbDatabase = dbDatabaseService.getDatabaseById(databaseId);
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo();
        BeanUtils.copyProperties(dbDatabase, databaseConnectionInfo, "password");

        // 2.测试数据源连接
        dbDatabaseService.connect(databaseConnectionInfo);

        // 3.获取当前数据源下所有表信息
        AbstractDatabaseConnector connector = dbDatabaseService.buildDbConnector(databaseConnectionInfo);
        Map<String, TableSchema> tableInfoMap = connector.getTableSchema(null, null);

        if (tableInfoMap == null || tableInfoMap.isEmpty()) {
            log.warn("数据库 {} 中没有找到表信息", databaseConnectionInfo.getDbName());
            return;
        }

        // 4.获取用户信息
        String currentUser = UserContext.getCurrentAccount();

        // 5.同步表信息到db_table表
        syncTablesToDatabase(databaseId, tableInfoMap, currentUser);

        // 6.同步字段信息到db_field表
        List<DbTableProperties> primaryKeyList = new ArrayList<>();
        syncFieldsToDatabase(databaseId, tableInfoMap, currentUser, primaryKeyList);

        // 7.同步主键信息到db_table_properties表
        syncPrimaryKeysToDatabase(databaseId, primaryKeyList, currentUser);
    }

    /**
     * 同步表信息到db_table表
     */
    private void syncTablesToDatabase(int databaseId, Map<String, TableSchema> tableInfoMap, String currentUser) {
        // 获取数据库中已存在的表信息
        List<DatabaseTable> existingTables = dbTableService.getTablesByDbId(databaseId);
        Map<String, DatabaseTable> existingTableMap = new HashMap<>();
        for (DatabaseTable table : existingTables) {
            existingTableMap.put(table.getName(), table);
        }

        Date currentTime = new Date();

        for (TableSchema sourceTableSchema : tableInfoMap.values()) {
            DatabaseTable targetTableInfo = existingTableMap.get(sourceTableSchema.getTableName());
            DbTable dbTableEntity = new DbTable();

            if (targetTableInfo == null) {
                // 新增表
                targetTableInfo = new DatabaseTable();
                targetTableInfo.setDbId(databaseId);
                targetTableInfo.setName(sourceTableSchema.getTableName());
                targetTableInfo.setBusinessName(sourceTableSchema.getTableName());
                targetTableInfo.setDescription(sourceTableSchema.getTableComment());
                targetTableInfo.setDeleted(Constants.DELETE_FLAG.FALSE);
                targetTableInfo.setCreatedBy(currentUser);
                targetTableInfo.setCreatedTime(currentTime);
                targetTableInfo.setUpdatedBy(currentUser);
                targetTableInfo.setUpdatedTime(currentTime);

                BeanUtils.copyProperties(targetTableInfo, dbTableEntity);
                dbTableService.save(dbTableEntity);
                // 设置生成的ID回写到targetTableInfo
                targetTableInfo.setId(dbTableEntity.getId());
            } else {
                // 更新表信息
                targetTableInfo.setDescription(sourceTableSchema.getTableComment());
                targetTableInfo.setUpdatedBy(currentUser);
                targetTableInfo.setUpdatedTime(currentTime);

                BeanUtils.copyProperties(targetTableInfo, dbTableEntity);
                dbTableService.updateById(dbTableEntity);
            }
        }

        // 处理已删除的表（标记为删除）
        Set<String> currentTableNames = tableInfoMap.keySet();
        for (DatabaseTable existingTable : existingTables) {
            if (!currentTableNames.contains(existingTable.getName())) {
                existingTable.setDeleted(Constants.DELETE_FLAG.TRUE);
                existingTable.setUpdatedBy(currentUser);
                existingTable.setUpdatedTime(currentTime);

                DbTable dbTableEntity = new DbTable();
                BeanUtils.copyProperties(existingTable, dbTableEntity);
                dbTableService.updateById(dbTableEntity);
            }
        }
    }

    /**
     * 同步字段信息到db_field表
     */
    private void syncFieldsToDatabase(int databaseId, Map<String, TableSchema> tableInfoMap,
                                      String currentUser, List<DbTableProperties> primaryKeyList) {
        // 获取所有表的ID映射
        Map<String, Integer> tableIdMap = new HashMap<>();
        List<DatabaseTable> dbTables = dbTableService.getTablesByDbId(databaseId);
        for (DatabaseTable table : dbTables) {
            if (table.getDeleted() == Constants.DELETE_FLAG.FALSE) {
                tableIdMap.put(table.getName(), table.getId());
            }
        }

        Date currentTime = new Date();

        for (TableSchema sourceTableSchema : tableInfoMap.values()) {
            Integer tableId = tableIdMap.get(sourceTableSchema.getTableName());
            if (tableId == null) {
                continue; // 表不存在，跳过字段同步
            }

            // 获取表中已存在的字段信息
            List<DatabaseField> existingFields = dbFieldService.getFieldsByTableId(tableId);
            Map<String, DatabaseField> existingFieldMap = new HashMap<>();
            for (DatabaseField field : existingFields) {
                if (field.getDeleted() == Constants.DELETE_FLAG.FALSE) {
                    existingFieldMap.put(field.getName(), field);
                }
            }

            // 同步字段信息
            Map<String, TableColumnSchema> columnInfoMap = sourceTableSchema.getColumnInfos();
            if (columnInfoMap != null) {
                for (TableColumnSchema sourceTableColumnSchema : columnInfoMap.values()) {
                    DatabaseField targetFieldInfo = existingFieldMap.get(sourceTableColumnSchema.getColumnName());
                    DbField dbFieldEntity = new DbField();

                    if (targetFieldInfo == null) {
                        // 新增字段
                        targetFieldInfo = new DatabaseField();
                        targetFieldInfo.setTableId(tableId);
                        targetFieldInfo.setName(sourceTableColumnSchema.getColumnName());
                        targetFieldInfo.setBusinessName(sourceTableColumnSchema.getColumnName());
                        targetFieldInfo.setDataType(sourceTableColumnSchema.getDataType());
                        targetFieldInfo.setDataLength(sourceTableColumnSchema.getDataLength());
                        targetFieldInfo.setDescription(sourceTableColumnSchema.getColumnComment());
                        targetFieldInfo.setNullable(sourceTableColumnSchema.getIsNull());
                        targetFieldInfo.setOrderBy(sourceTableColumnSchema.getColumnOrder());
                        targetFieldInfo.setPrecision(sourceTableColumnSchema.getPrecision());
                        targetFieldInfo.setDeleted(Constants.DELETE_FLAG.FALSE);
                        targetFieldInfo.setCreatedBy(currentUser);
                        targetFieldInfo.setCreatedTime(currentTime);
                        targetFieldInfo.setUpdatedBy(currentUser);
                        targetFieldInfo.setUpdatedTime(currentTime);

                        BeanUtils.copyProperties(targetFieldInfo, dbFieldEntity);
                        dbFieldService.save(dbFieldEntity);
                    } else {
                        // 更新字段信息
                        targetFieldInfo.setDataType(sourceTableColumnSchema.getDataType());
                        targetFieldInfo.setDataLength(sourceTableColumnSchema.getDataLength());
                        targetFieldInfo.setDescription(sourceTableColumnSchema.getColumnComment());
                        targetFieldInfo.setNullable(sourceTableColumnSchema.getIsNull());
                        targetFieldInfo.setOrderBy(sourceTableColumnSchema.getColumnOrder());
                        targetFieldInfo.setPrecision(sourceTableColumnSchema.getPrecision());
                        targetFieldInfo.setUpdatedBy(currentUser);
                        targetFieldInfo.setUpdatedTime(currentTime);

                        BeanUtils.copyProperties(targetFieldInfo, dbFieldEntity);
                        dbFieldService.updateById(dbFieldEntity);
                    }

                    // 收集主键信息
                    collectPrimaryKey(sourceTableColumnSchema, tableId, primaryKeyList);
                }
            }

            // 处理已删除的字段（标记为删除）
            Set<String> currentFieldNames = columnInfoMap != null ? columnInfoMap.keySet() : Collections.emptySet();
            for (DatabaseField existingField : existingFields) {
                if (!currentFieldNames.contains(existingField.getName())) {
                    existingField.setDeleted(Constants.DELETE_FLAG.TRUE);
                    existingField.setUpdatedBy(currentUser);
                    existingField.setUpdatedTime(currentTime);

                    DbField dbFieldEntity = new DbField();
                    BeanUtils.copyProperties(existingField, dbFieldEntity);
                    dbFieldService.updateById(dbFieldEntity);
                }
            }
        }
    }

    /**
     * 收集主键信息
     */
    private void collectPrimaryKey(TableColumnSchema columnSchema, Integer tableId,
                                   List<DbTableProperties> primaryKeyList) {
        // MySQLConnector.getConstraintType(): 主键返回 "P"（不是 "PRI"）
        String constraintType = columnSchema.getConstraintType();
        boolean isPrimaryKey = "P".equalsIgnoreCase(constraintType) || "PRI".equalsIgnoreCase(constraintType);
        if (isPrimaryKey) {
            DbTableProperties primaryKeyProp = new DbTableProperties();
            primaryKeyProp.setId(CommonUtil.getUUIDString());
            primaryKeyProp.setTableId(tableId);
            primaryKeyProp.setPropType(Constants.TABLE_PROPERTIES.PRIMARY_KEY);
            primaryKeyProp.setPropValues(columnSchema.getColumnName());
            primaryKeyList.add(primaryKeyProp);
        }
    }

    /**
     * 同步主键信息到db_table_properties表
     */
    private void syncPrimaryKeysToDatabase(int databaseId, List<DbTableProperties> primaryKeyList,
                                           String currentUser) {
        if (primaryKeyList.isEmpty()) {
            return;
        }

        // 按表ID分组，处理复合主键
        Map<Integer, List<DbTableProperties>> groupedByTableId = primaryKeyList.stream()
                .collect(Collectors.groupingBy(DbTableProperties::getTableId));

        List<DbTableProperties> finalPrimaryKeyList = new ArrayList<>();
        Date currentTime = new Date();

        for (Map.Entry<Integer, List<DbTableProperties>> entry : groupedByTableId.entrySet()) {
            Integer tableId = entry.getKey();
            List<DbTableProperties> tablePrimaryKeys = entry.getValue();

            // 如果有多个主键，需要拼接成逗号分隔的字符串
            if (tablePrimaryKeys.size() > 1) {
                String combinedPrimaryKeys = tablePrimaryKeys.stream()
                        .map(DbTableProperties::getPropValues)
                        .collect(Collectors.joining(","));

                DbTableProperties combinedProp = new DbTableProperties();
                combinedProp.setId(CommonUtil.getUUIDString());
                combinedProp.setTableId(tableId);
                combinedProp.setPropType(Constants.TABLE_PROPERTIES.PRIMARY_KEY);
                combinedProp.setPropValues(combinedPrimaryKeys);
                combinedProp.setCreatedBy(currentUser);
                combinedProp.setCreatedTime(currentTime);
                finalPrimaryKeyList.add(combinedProp);
            } else {
                // 单个主键，直接使用
                DbTableProperties singleProp = tablePrimaryKeys.get(0);
                if (singleProp.getId() == null || singleProp.getId().trim().isEmpty()) {
                    singleProp.setId(UUID.randomUUID().toString());
                }
                singleProp.setCreatedBy(currentUser);
                singleProp.setCreatedTime(currentTime);
                finalPrimaryKeyList.add(singleProp);
            }
        }

        // 先删除该数据库下所有表的主键配置
        dbTablePropertiesService.deleteByDatabaseId(databaseId);

        // 批量插入新的主键配置
        if (!finalPrimaryKeyList.isEmpty()) {
            dbTablePropertiesService.batchInsert(finalPrimaryKeyList);
        }
    }
}