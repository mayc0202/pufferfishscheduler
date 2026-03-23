package com.pufferfishscheduler.worker.task.metadata.connect.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.ConnectorUtil;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.domain.domain.TableColumnSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.worker.task.metadata.connect.AbstractDatabaseConnector;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Worker 侧 MySQL 元数据采集连接器。
 */
public class MySQLConnector extends AbstractDatabaseConnector {

    private static final String TABLE_INFO_SQL =
            "SELECT TABLE_NAME,TABLE_COMMENT,TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? ";
    private static final String COLUMN_INFO_SQL =
            "SELECT TABLE_NAME,COLUMN_NAME,COLUMN_COMMENT,COLUMN_TYPE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,IS_NULLABLE,COLUMN_KEY,ORDINAL_POSITION " +
                    "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? ";

    @Override
    public AbstractDatabaseConnector build() {
        setDriver(JdbcUtil.getDriver(Constants.DATABASE_TYPE.MYSQL));
        setUrl(JdbcUtil.getUrl(Constants.DATABASE_TYPE.MYSQL, getHost(), String.valueOf(getPort()), getDbName(), getExtConfig()));
        return this;
    }

    @Override
    public Connection getConnection() {
        try {
            return JdbcUtil.getConnection(getDriver(), getUrl(), getDatabaseInfo());
        } catch (Exception e) {
            throw new BusinessException("创建数据库连接失败: " + e.getMessage());
        }
    }

    @Override
    public Map<String, TableSchema> getTableSchema(List<String> inTableNames, List<String> notInTableNames) {
        Map<String, TableSchema> tableMap = getTables(inTableNames, notInTableNames);
        List<TableColumnSchema> columns = getColumns(inTableNames, notInTableNames);
        if (tableMap.isEmpty() || columns.isEmpty()) {
            return tableMap;
        }
        for (TableColumnSchema c : columns) {
            TableSchema t = tableMap.get(c.getTableName());
            if (t == null) {
                continue;
            }
            if (t.getColumnInfos() == null) {
                t.setColumnInfos(new HashMap<>());
            }
            t.getColumnInfos().put(c.getColumnName(), c);
        }
        return tableMap;
    }

    private Map<String, TableSchema> getTables(List<String> inTables, List<String> notInTables) {
        Map<String, TableSchema> result = new HashMap<>();
        List<String> params = new ArrayList<>();
        params.add(getDbName());
        String sql = buildTableSql(TABLE_INFO_SQL, inTables, notInTables, params);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setString(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TableSchema tableSchema = new TableSchema();
                    tableSchema.setTableName(rs.getString("TABLE_NAME"));
                    tableSchema.setTableComment(rs.getString("TABLE_COMMENT"));
                    tableSchema.setTableType(rs.getString("TABLE_TYPE"));
                    result.put(tableSchema.getTableName(), tableSchema);
                }
            }
        } catch (Exception e) {
            throw new BusinessException("获取表结构失败: " + e.getMessage());
        }
        return result;
    }

    private List<TableColumnSchema> getColumns(List<String> inTables, List<String> notInTables) {
        List<TableColumnSchema> result = new ArrayList<>();
        List<String> params = new ArrayList<>();
        params.add(getDbName());
        String sql = buildTableSql(COLUMN_INFO_SQL, inTables, notInTables, params);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setString(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TableColumnSchema c = new TableColumnSchema();
                    c.setTableName(rs.getString("TABLE_NAME"));
                    c.setColumnName(rs.getString("COLUMN_NAME"));
                    c.setColumnComment(rs.getString("COLUMN_COMMENT"));
                    c.setDataType(rs.getString("DATA_TYPE"));
                    c.setConstraintType(getConstraintType(rs.getString("COLUMN_KEY")));
                    Object scale = rs.getObject("NUMERIC_SCALE");
                    if (scale instanceof BigInteger) {
                        int v = ((BigInteger) scale).intValue();
                        if (v != 0) c.setPrecision(v);
                    } else if (scale instanceof Long) {
                        int v = ((Long) scale).intValue();
                        if (v != 0) c.setPrecision(v);
                    }
                    String dataLength = ConnectorUtil.getMsg(rs.getString("COLUMN_TYPE"));
                    if (StringUtils.isNotBlank(dataLength)) {
                        c.setDataLength(Integer.valueOf(dataLength));
                    }
                    c.setIsNull(!"NO".equals(rs.getString("IS_NULLABLE")));
                    c.setColumnOrder(Integer.valueOf(rs.getString("ORDINAL_POSITION")));
                    result.add(c);
                }
            }
        } catch (Exception e) {
            throw new BusinessException("获取列信息失败: " + e.getMessage());
        }
        return result;
    }

    private String buildTableSql(String sql, List<String> inTables, List<String> notInTables, List<String> params) {
        if (inTables != null && !inTables.isEmpty()) {
            StringBuilder cond = new StringBuilder();
            sql = sql + " AND TABLE_NAME IN (";
            for (int i = 0; i < inTables.size(); i++) {
                cond.append("?");
                if (i < inTables.size() - 1) cond.append(",");
                params.add(inTables.get(i));
            }
            sql = sql + cond + ")";
        }
        if (notInTables != null && !notInTables.isEmpty()) {
            StringBuilder cond = new StringBuilder();
            sql = sql + " AND TABLE_NAME NOT IN (";
            for (int i = 0; i < notInTables.size(); i++) {
                cond.append("?");
                if (i < notInTables.size() - 1) cond.append(",");
                params.add(notInTables.get(i));
            }
            sql = sql + cond + ")";
        }
        return sql;
    }

    private String getConstraintType(String columnKey) {
        if ("PRI".equals(columnKey)) return "P";
        if ("MUL".equals(columnKey)) return "R";
        if ("UNI".equals(columnKey)) return "U";
        return "C";
    }
}

