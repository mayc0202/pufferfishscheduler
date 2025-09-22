package com.pufferfishscheduler.service.database.db.sync;

import com.pufferfishscheduler.common.bean.EtlApplicationContext;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.DbField;
import com.pufferfishscheduler.dao.entity.DbTable;
import com.pufferfishscheduler.domain.model.DatabaseConnectionInfo;
import com.pufferfishscheduler.domain.model.DatabaseField;
import com.pufferfishscheduler.domain.model.DatabaseTable;
import com.pufferfishscheduler.service.database.db.connector.relationdb.AbstractDatabaseConnector;
import com.pufferfishscheduler.domain.domain.TableColumnSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.service.database.db.service.DbDatabaseService;
import com.pufferfishscheduler.service.database.db.service.DbFieldService;
import com.pufferfishscheduler.service.database.db.service.DbTableService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.util.*;

/**
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

    /**
     * 同步执行，定时任务执行使用
     *
     * @param databaseId 数据库ID
     * @throws SQLException
     */
    public void execute(int databaseId) {
        DbSyncExecutor executor = EtlApplicationContext.getBean(DbSyncExecutor.class);
        // 同步表基本信息
        executor.syncTableInfo(databaseId);
    }

    @Transactional
    public void syncTableInfo(int databaseId) {
        // 1.根据数据源ID获取数据源信息
        DbDatabase dbDatabase = dbDatabaseService.getDatabaseById(databaseId);
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo();
        BeanUtils.copyProperties(dbDatabase, databaseConnectionInfo,"password");

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
        syncFieldsToDatabase(databaseId, tableInfoMap, currentUser);
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
                targetTableInfo.setDeleted(false);
                targetTableInfo.setCreatedBy(currentUser);
                targetTableInfo.setCreatedTime(currentTime);
                targetTableInfo.setUpdatedBy(currentUser);
                targetTableInfo.setUpdatedTime(currentTime);

                BeanUtils.copyProperties(targetTableInfo, dbTableEntity);
                dbTableService.save(dbTableEntity);
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
    private void syncFieldsToDatabase(int databaseId, Map<String, TableSchema> tableInfoMap, String currentUser) {
        // 获取所有表的ID映射
        Map<String, Integer> tableIdMap = new HashMap<>();
        List<DatabaseTable> dbTables = dbTableService.getTablesByDbId(databaseId);
        for (DatabaseTable table : dbTables) {
            tableIdMap.put(table.getName(), table.getId());
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
                existingFieldMap.put(field.getName(), field);
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
                        targetFieldInfo.setDeleted(false);
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
}
