package com.pufferfishscheduler.master.database.connect.relationdb.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.domain.domain.TableColumnSchema;
import com.pufferfishscheduler.domain.domain.TableForeignKey;
import com.pufferfishscheduler.domain.domain.TableIndexSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.master.database.connect.relationdb.AbstractDatabaseConnector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * PostgreSQL连接器
 */
@Slf4j
public class PostgreSQLConnector extends AbstractDatabaseConnector {

    private static final String DRIVER_CLASS = "org.postgresql.Driver";

    /**
     * 表信息SQL
     */
    private static final String TABLE_INFO_SQL =
            "SELECT relname AS TABLE_NAME, " +
                    "       obj_description(oid) AS TABLE_COMMENT, " +
                    "       CASE WHEN relkind = 'r' THEN 'BASE TABLE' ELSE 'VIEW' END AS TABLE_TYPE " +
                    "FROM pg_class " +
                    "LEFT JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid " +
                    "WHERE nspname = ? AND relkind IN ('r','v') " +
                    "AND relname NOT IN (SELECT inhrelid FROM pg_inherits)";

    /**
     * 列信息SQL
     */
    private static final String COLUMN_INFO_SQL =
            "SELECT c.table_name, " +
                    "       c.column_name, " +
                    "       d.description AS column_comment, " +
                    "       c.data_type, " +
                    "       c.character_maximum_length, " +
                    "       c.numeric_precision, " +
                    "       c.numeric_scale, " +
                    "       c.is_nullable, " +
                    "       c.ordinal_position, " +
                    "       t.constraint_type " +
                    "FROM information_schema.columns c " +
                    "LEFT JOIN pg_attribute a ON a.attname = c.column_name " +
                    "LEFT JOIN pg_class cl ON cl.relname = c.table_name AND cl.relnamespace = a.attrelid " +
                    "LEFT JOIN pg_description d ON d.objoid = cl.oid AND d.objsubid = a.attnum " +
                    "LEFT JOIN ( " +
                    "    SELECT table_name, column_name, constraint_type " +
                    "    FROM information_schema.table_constraints tc " +
                    "    JOIN information_schema.constraint_column_usage ccu " +
                    "    ON tc.constraint_name = ccu.constraint_name " +
                    ") t ON c.table_name = t.table_name AND c.column_name = t.column_name " +
                    "WHERE c.table_schema = ?";

    /**
     * 外键SQL
     */
    private static final String FK_CONSTRAINT_TABLE_SQL =
            "SELECT tc.constraint_name, " +
                    "       tc.table_name, " +
                    "       ccu.table_name AS referenced_table_name, " +
                    "       array_to_string(array_agg(kcu.column_name), ',') AS column_name, " +
                    "       array_to_string(array_agg(ccu.column_name), ',') AS referenced_column_name " +
                    "FROM information_schema.table_constraints tc " +
                    "JOIN information_schema.key_column_usage kcu " +
                    "ON tc.constraint_name = kcu.constraint_name " +
                    "JOIN information_schema.constraint_column_usage ccu " +
                    "ON tc.constraint_name = ccu.constraint_name " +
                    "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = ? " +
                    "GROUP BY tc.constraint_name, tc.table_name, ccu.table_name";

    /**
     * 索引SQL
     */
    private static final String IDX_CONSTRAINT_TABLE_SQL =
            "SELECT tablename AS table_name, " +
                    "       indexname AS index_name, " +
                    "       NOT indexdef LIKE '%UNIQUE%' AS non_unique, " +
                    "       substring(indexdef from '\\((.*?)\\)') AS column_name " +
                    "FROM pg_indexes " +
                    "WHERE schemaname = ?";

    /**
     * 主键表查询
     */
    private static final String PRIMARY_CONSTRAINT_TABLE_SQL =
            "SELECT DISTINCT table_name " +
                    "FROM information_schema.table_constraints " +
                    "WHERE constraint_type = 'PRIMARY KEY' AND table_schema = ?";

    /**
     * 构建连接器
     */
    @Override
    public AbstractDatabaseConnector build() {
        this.setDriver(JdbcUtil.getDriver(Constants.DATABASE_TYPE.POSTGRESQL));
        this.setUrl(JdbcUtil.getUrl(Constants.DATABASE_TYPE.POSTGRESQL, getHost(), String.valueOf(getPort()), getDbName(), getExtConfig()));
        return this;
    }

    /**
     * 获取数据库连接
     */
    @Override
    public Connection getConnection() {
        Connection conn;
        try {
            conn = JdbcUtil.getConnection(getDriver(), getUrl(), getDatabaseInfo());
        } catch (Exception e) {
            log.error("创建PostgreSQL连接失败", e);
            throw new BusinessException("创建数据库连接失败！" + e.getMessage());
        }
        return conn;
    }

    /**
     * 获取外键信息
     */
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
                fk.setName(rs.getString("constraint_name"));
                fk.setTableName(rs.getString("table_name"));
                fk.setRefTableName(rs.getString("referenced_table_name"));
                fk.setColumnNames(rs.getString("column_name"));
                fk.setRefColumnNames(rs.getString("referenced_column_name"));
                result.add(fk);
            }
        } catch (Exception e) {
            log.error("获取PostgreSQL外键信息失败", e);
            throw new BusinessException("获取外键信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

    /**
     * 获取索引信息
     */
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
                index.setName(rs.getString("index_name"));
                index.setTableName(rs.getString("table_name"));
                index.setUnique(!rs.getBoolean("non_unique"));
                index.setColumnNames(rs.getString("column_name"));
                result.add(index);
            }
        } catch (Exception e) {
            log.error("获取PostgreSQL索引信息失败", e);
            throw new BusinessException("获取索引信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

    /**
     * 获取表信息
     * @param inTables    仅包含 inTableNames 指定的表
     * @param notInTables 排除 notInTableNames 包含的表
     * @return
     */
    @Override
    public Map<String, TableSchema> getTableSchema(List<String> inTables, List<String> notInTables) {
        Map<String, TableSchema> tableInfos = getTables(inTables, notInTables);
        List<TableColumnSchema> columnList = getColumnInfos(inTables, notInTables);

        if (tableInfos.isEmpty() || columnList.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, TableSchema> result = new HashMap<>();
        copyToDBTableWithColumnMap(tableInfos, result);

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

    private Map<String, TableSchema> getTables(List<String> inTables, List<String> notInTables) {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        Map<String, TableSchema> result = new HashMap<>();
        List<String> params = new ArrayList<>();

        try {
            params.add(getSchema());
            conn = getConnection();
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
                table.setTableType(getTableType(rs.getString("TABLE_TYPE")));
                result.put(table.getTableName(), table);
            }
        } catch (Exception e) {
            log.error("获取PostgreSQL表信息失败", e);
            throw new BusinessException("获取表结构信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(stat);
            JdbcUtil.close(conn);
        }
        return result;
    }

    /**
     * 获取列信息
     */
    private List<TableColumnSchema> getColumnInfos(List<String> inTables, List<String> notInTables) {
        List<String> primaryTables = getPrimaryConstraintTableList();
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<TableColumnSchema> result = new ArrayList<>();
        List<String> params = new ArrayList<>();

        try {
            params.add(getSchema());
            conn = getConnection();
            String sql = buildTableSql(COLUMN_INFO_SQL, inTables, notInTables, params);
            stat = conn.prepareStatement(sql);

            for (int i = 0; i < params.size(); i++) {
                stat.setString(i + 1, params.get(i));
            }

            rs = stat.executeQuery();
            while (rs.next()) {
                TableColumnSchema col = new TableColumnSchema();
                String tableName = rs.getString("table_name");
                col.setTableName(tableName);
                col.setColumnName(rs.getString("column_name"));
                col.setColumnComment(rs.getString("column_comment"));
                col.setDataType(rs.getString("data_type"));
                col.setIsNull(!"NO".equals(rs.getString("is_nullable")));
                col.setColumnOrder(rs.getInt("ordinal_position"));

                // 长度/精度
                int len = rs.getInt("character_maximum_length");
                if (rs.wasNull()) len = rs.getInt("numeric_precision");
                col.setDataLength(len > 0 ? len : null);

                int scale = rs.getInt("numeric_scale");
                if (!rs.wasNull() && scale > 0) col.setPrecision(scale);

                // 约束类型
                String constraintType = rs.getString("constraint_type");
                col.setConstraintType(getConstraintType(constraintType, tableName, primaryTables));

                result.add(col);
            }
        } catch (Exception e) {
            log.error("获取PostgreSQL列信息失败", e);
            throw new BusinessException("获取列信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

    /**
     * 获取包含主键约束的表信息
     */
    private List<String> getPrimaryConstraintTableList() {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<String> result = new ArrayList<>();

        try {
            conn = getConnection();
            stat = conn.prepareStatement(PRIMARY_CONSTRAINT_TABLE_SQL);
            stat.setString(1, getSchema());
            rs = stat.executeQuery();

            while (rs.next()) {
                result.add(rs.getString("table_name"));
            }
        } catch (Exception e) {
            log.error("获取PostgreSQL主键表失败", e);
            throw new BusinessException("获取包含主键约束的表信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

    /**
     * 构建表信息查询SQL语句
     */
    private String buildTableSql(String sql, List<String> inTables, List<String> notInTables, List<String> params) {
        if (inTables != null && !inTables.isEmpty()) {
            sql += " AND table_name IN (" + String.join(",", Collections.nCopies(inTables.size(), "?")) + ")";
            params.addAll(inTables);
        }
        if (notInTables != null && !notInTables.isEmpty()) {
            sql += " AND table_name NOT IN (" + String.join(",", Collections.nCopies(notInTables.size(), "?")) + ")";
            params.addAll(notInTables);
        }
        return sql;
    }

    /**
     * 获取约束类型
     * @param constraintType
     * @param tableName
     * @param primaryTables
     * @return
     */
    private String getConstraintType(String constraintType, String tableName, List<String> primaryTables) {
        if ("PRIMARY KEY".equals(constraintType)) {
            return primaryTables.contains(tableName) ? "P" : "U";
        }
        if ("UNIQUE".equals(constraintType)) {
            return "U";
        }
        if ("FOREIGN KEY".equals(constraintType)) {
            return "R";
        }
        return "C";
    }

    /**
     * 获取表类型
     * @param tableType
     * @return
     */
    private String getTableType(String tableType) {
        return "BASE TABLE".equals(tableType) ? "TABLE" : "VIEW";
    }

    /**
     * 复制表信息到数据库表映射
     */
    private void copyToDBTableWithColumnMap(Map<String, TableSchema> src, Map<String, TableSchema> dest) {
        for (Map.Entry<String, TableSchema> entry : src.entrySet()) {
            TableSchema target = new TableSchema();
            BeanUtils.copyProperties(entry.getValue(), target);
            dest.put(entry.getKey(), target);
        }
    }
}