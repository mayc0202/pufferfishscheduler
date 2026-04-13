package com.pufferfishscheduler.worker.task.connect.relationdb.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.domain.domain.TableColumnSchema;
import com.pufferfishscheduler.domain.domain.TableForeignKey;
import com.pufferfishscheduler.domain.domain.TableIndexSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.worker.task.connect.relationdb.AbstractDatabaseConnector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * SQLServer数据库连接器
 */
@Slf4j
public class SQLServerConnector extends AbstractDatabaseConnector {

    /**
     * 表信息 SQL
     */
    private static final String TABLE_INFO_SQL =
            "SELECT " +
                    "    t.name AS TABLE_NAME, " +
                    "    ISNULL(ep.value, '') AS TABLE_COMMENT, " +
                    "    CASE WHEN t.type = 'U' THEN 'TABLE' ELSE 'VIEW' END AS TABLE_TYPE " +
                    "FROM sys.tables t " +
                    "LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description' " +
                    "WHERE SCHEMA_NAME(t.schema_id) = ? " +
                    "UNION ALL " +
                    "SELECT " +
                    "    v.name AS TABLE_NAME, " +
                    "    ISNULL(ep.value, '') AS TABLE_COMMENT, " +
                    "    'VIEW' AS TABLE_TYPE " +
                    "FROM sys.views v " +
                    "LEFT JOIN sys.extended_properties ep ON ep.major_id = v.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description' " +
                    "WHERE SCHEMA_NAME(v.schema_id) = ?";

    /**
     * 列信息 SQL
     */
    private static final String COLUMN_INFO_SQL =
            "SELECT " +
                    "    c.TABLE_NAME, " +
                    "    c.COLUMN_NAME, " +
                    "    ISNULL(ep.value, '') AS COLUMN_COMMENT, " +
                    "    c.DATA_TYPE, " +
                    "    c.CHARACTER_MAXIMUM_LENGTH, " +
                    "    c.NUMERIC_PRECISION, " +
                    "    c.NUMERIC_SCALE, " +
                    "    c.IS_NULLABLE, " +
                    "    c.ORDINAL_POSITION, " +
                    "    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END AS COLUMN_KEY " +
                    "FROM INFORMATION_SCHEMA.COLUMNS c " +
                    "LEFT JOIN ( " +
                    "    SELECT ku.TABLE_NAME, ku.COLUMN_NAME " +
                    "    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc " +
                    "    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku " +
                    "    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME AND tc.TABLE_NAME = ku.TABLE_NAME " +
                    "    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_SCHEMA = ? " +
                    ") pk ON c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME " +
                    "LEFT JOIN sys.extended_properties ep " +
                    "ON ep.major_id = OBJECT_ID(c.TABLE_NAME) AND ep.minor_id = c.ORDINAL_POSITION AND ep.name = 'MS_Description' " +
                    "WHERE c.TABLE_SCHEMA = ?";

    /**
     * 外键 SQL
     */
    private static final String FK_CONSTRAINT_TABLE_SQL =
            "SELECT " +
                    "    fk.name AS CONSTRAINT_NAME, " +
                    "    OBJECT_NAME(fk.parent_object_id) AS TABLE_NAME, " +
                    "    OBJECT_NAME(fk.referenced_object_id) AS REFERENCED_TABLE_NAME, " +
                    "    STRING_AGG(c1.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS COLUMN_NAME, " +
                    "    STRING_AGG(c2.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS REFERENCED_COLUMN_NAME " +
                    "FROM sys.foreign_keys fk " +
                    "JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id " +
                    "JOIN sys.columns c1 ON fkc.parent_object_id = c1.object_id AND fkc.parent_column_id = c1.column_id " +
                    "JOIN sys.columns c2 ON fkc.referenced_object_id = c2.object_id AND fkc.referenced_column_id = c2.column_id " +
                    "JOIN sys.tables t ON fk.parent_object_id = t.object_id " +
                    "WHERE SCHEMA_NAME(t.schema_id) = ? " +
                    "GROUP BY fk.name, fk.parent_object_id, fk.referenced_object_id";

    /**
     * 索引 SQL
     */
    private static final String IDX_CONSTRAINT_TABLE_SQL =
            "SELECT " +
                    "    i.name AS INDEX_NAME, " +
                    "    OBJECT_NAME(i.object_id) AS TABLE_NAME, " +
                    "    i.is_unique AS NON_UNIQUE, " +
                    "    STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.index_column_id) AS COLUMN_NAME " +
                    "FROM sys.indexes i " +
                    "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id " +
                    "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id " +
                    "JOIN sys.tables t ON i.object_id = t.object_id " +
                    "WHERE i.type > 0 AND i.is_primary_key = 0 AND SCHEMA_NAME(t.schema_id) = ? " +
                    "GROUP BY i.name, i.object_id, i.is_unique";

    @Override
    public AbstractDatabaseConnector build() {
        this.setDriver(JdbcUtil.getDriver(Constants.DATABASE_TYPE.SQL_SERVER));
        String url = JdbcUtil.getUrl(Constants.DATABASE_TYPE.SQL_SERVER, getHost(), String.valueOf(getPort()), getDbName(), getExtConfig());
        this.setUrl(url);
        return this;
    }

    @Override
    public Connection getConnection() {
        try {
            return JdbcUtil.getConnection(getDriver(), getUrl(), getDatabaseInfo());
        } catch (Exception e) {
            log.error("SQLServer连接创建失败", e);
            throw new BusinessException("创建数据库连接失败！" + e.getMessage());
        }
    }

    @Override
    public Map<String, TableSchema> getTableSchema(List<String> inTableNames, List<String> notInTableNames) {
        Map<String, TableSchema> tableInfos = getTables(inTableNames, notInTableNames);
        List<TableColumnSchema> columnList = getColumnInfos(inTableNames, notInTableNames);

        if (tableInfos.isEmpty() || columnList == null || columnList.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, TableSchema> result = new HashMap<>();
        copyToDBTableMap(tableInfos, result);

        for (TableColumnSchema col : columnList) {
            TableSchema table = result.get(col.getTableName());
            if (table == null) continue;

            Map<String, TableColumnSchema> columnMap = table.getColumnInfos();
            if (columnMap == null) columnMap = new HashMap<>();
            columnMap.put(col.getColumnName(), col);
            table.setColumnInfos(columnMap);
        }
        return result;
    }

    @Override
    public List<TableForeignKey> getForeignKeys() {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<TableForeignKey> result = new ArrayList<>();

        try {
            conn = getConnection();
            stat = conn.prepareStatement(FK_CONSTRAINT_TABLE_SQL);
            stat.setString(1, getSchema());
            rs = stat.executeQuery();

            while (rs.next()) {
                TableForeignKey fk = new TableForeignKey();
                fk.setName(rs.getString("CONSTRAINT_NAME"));
                fk.setTableName(rs.getString("TABLE_NAME"));
                fk.setRefTableName(rs.getString("REFERENCED_TABLE_NAME"));
                fk.setColumnNames(rs.getString("COLUMN_NAME"));
                fk.setRefColumnNames(rs.getString("REFERENCED_COLUMN_NAME"));
                result.add(fk);
            }
        } catch (Exception e) {
            log.error("获取SQLServer外键失败", e);
            throw new BusinessException("获取外键信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

    @Override
    public List<TableIndexSchema> getIndexes() {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<TableIndexSchema> result = new ArrayList<>();

        try {
            conn = getConnection();
            stat = conn.prepareStatement(IDX_CONSTRAINT_TABLE_SQL);
            stat.setString(1, getSchema());
            rs = stat.executeQuery();

            while (rs.next()) {
                TableIndexSchema index = new TableIndexSchema();
                index.setName(rs.getString("INDEX_NAME"));
                index.setTableName(rs.getString("TABLE_NAME"));
                index.setUnique(rs.getBoolean("NON_UNIQUE"));
                index.setColumnNames(rs.getString("COLUMN_NAME"));
                result.add(index);
            }
        } catch (Exception e) {
            log.error("获取SQLServer索引失败", e);
            throw new BusinessException("获取索引信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

    // ===================== 内部工具方法（完全对齐 Oracle/MySQL） =====================

    private Map<String, TableSchema> getTables(List<String> inTables, List<String> notInTables) {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        Map<String, TableSchema> result = new HashMap<>();
        List<String> params = new ArrayList<>();

        try {
            conn = getConnection();
            String schema = getSchema();
            params.add(schema);
            params.add(schema);

            String sql = buildTableSql(TABLE_INFO_SQL, inTables, notInTables, params);
            stat = conn.prepareStatement(sql);

            for (int i = 0; i < params.size(); i++) {
                stat.setString(i + 1, params.get(i));
            }

            rs = stat.executeQuery();
            while (rs.next()) {
                TableSchema table = new TableSchema();
                table.setTableName(rs.getString("TABLE_NAME"));
                table.setTableComment(rs.getString("TABLE_COMMENT"));
                table.setTableType(rs.getString("TABLE_TYPE"));
                result.put(table.getTableName(), table);
            }
        } catch (Exception e) {
            log.error("获取SQLServer表信息失败", e);
            throw new BusinessException("获取表结构信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(stat);
            JdbcUtil.close(conn);
        }
        return result;
    }

    private List<TableColumnSchema> getColumnInfos(List<String> inTables, List<String> notInTables) {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<TableColumnSchema> result = new ArrayList<>();
        List<String> params = new ArrayList<>();

        try {
            conn = getConnection();
            String schema = getSchema();
            params.add(schema);
            params.add(schema);

            String sql = buildTableSql(COLUMN_INFO_SQL, inTables, notInTables, params);
            stat = conn.prepareStatement(sql);

            for (int i = 0; i < params.size(); i++) {
                stat.setString(i + 1, params.get(i));
            }

            rs = stat.executeQuery();
            while (rs.next()) {
                TableColumnSchema col = new TableColumnSchema();
                col.setTableName(rs.getString("TABLE_NAME"));
                col.setColumnName(rs.getString("COLUMN_NAME"));
                col.setColumnComment(rs.getString("COLUMN_COMMENT"));
                col.setDataType(rs.getString("DATA_TYPE"));
                col.setIsNull(!"NO".equals(rs.getString("IS_NULLABLE")));
                col.setColumnOrder(rs.getInt("ORDINAL_POSITION"));

                // 长度
                Integer len = rs.getInt("CHARACTER_MAXIMUM_LENGTH");
                if (!rs.wasNull() && len > 0) {
                    col.setDataLength(len);
                }

                // 精度
                Object scaleObj = rs.getObject("NUMERIC_SCALE");
                if (scaleObj != null) {
                    col.setPrecision(Integer.parseInt(String.valueOf(scaleObj)));
                }

                // 约束类型
                col.setConstraintType(getConstraintType(rs.getString("COLUMN_KEY")));
                result.add(col);
            }
        } catch (Exception e) {
            log.error("获取SQLServer列信息失败", e);
            throw new BusinessException("获取列信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

    private String getConstraintType(String columnKey) {
        if ("PRI".equals(columnKey)) {
            return "P";
        } else if ("UNI".equals(columnKey)) {
            return "U";
        } else {
            return "C";
        }
    }

    private String buildTableSql(String sql, List<String> inTables, List<String> notInTables, List<String> params) {
        if (inTables != null && !inTables.isEmpty()) {
            sql += " AND TABLE_NAME IN (" + String.join(",", Collections.nCopies(inTables.size(), "?")) + ")";
            params.addAll(inTables);
        }
        if (notInTables != null && !notInTables.isEmpty()) {
            sql += " AND TABLE_NAME NOT IN (" + String.join(",", Collections.nCopies(notInTables.size(), "?")) + ")";
            params.addAll(notInTables);
        }
        return sql;
    }

    private void copyToDBTableMap(Map<String, TableSchema> src, Map<String, TableSchema> dest) {
        for (Map.Entry<String, TableSchema> entry : src.entrySet()) {
            TableSchema target = new TableSchema();
            BeanUtils.copyProperties(entry.getValue(), target);
            dest.put(entry.getKey(), target);
        }
    }
}