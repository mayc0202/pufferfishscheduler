package com.pufferfishscheduler.worker.task.connect.relationdb.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.ConnectorUtil;
import com.pufferfishscheduler.common.utils.JdbcUrlUtil;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Doris数据库连接器
 *
 * @author
 * @since 2026-03-30
 */
@Slf4j
public class DorisConnector extends AbstractDatabaseConnector {

    /**
     * 表信息
     */
    private static final String TABLE_INFO_SQL = "SELECT `TABLE_NAME`,TABLE_COMMENT,TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? ";

    /**
     * 列信息
     */
    private static final String COLUMN_INFO_SQL =
            "SELECT TABLE_NAME,COLUMN_NAME,COLUMN_COMMENT,COLUMN_TYPE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,IS_NULLABLE,COLUMN_KEY,ORDINAL_POSITION" +
                    " FROM INFORMATION_SCHEMA.COLUMNS  " +
                    "WHERE TABLE_SCHEMA = ? ";

    /**
     * 获取含有唯一键约束的表信息（Doris中唯一键作为主键使用）
     */
    private static final String UNIQUE_CONSTRAINT_TABLE_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_KEY = 'UNI' AND TABLE_SCHEMA = ?";

    /**
     * 获取建表语句
     */
    private static final String CREATE_TABLE_SQL = "SHOW CREATE TABLE `%s`";

    /**
     * 获取索引信息
     */
    private static final String SHOW_INDEX_SQL = "SHOW INDEX FROM `%s`";

    private Pattern propertiesPattern;

    @Override
    public AbstractDatabaseConnector build() {
        this.setDriver(JdbcUrlUtil.getDriver(Constants.DATABASE_TYPE.DORIS));
        String url = JdbcUtil.getUrl(Constants.DATABASE_TYPE.DORIS, getHost(), String.valueOf(getPort()), getDbName(), getExtConfig());
        this.setUrl(url);
        return this;
    }

    @Override
    public Connection getConnection() {
        try {
            return JdbcUtil.getConnection(getDriver(), getUrl(), getDatabaseInfo());
        } catch (Exception e) {
            log.error("Doris数据库连接创建失败", e);
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
        copyToTableSchemaMap(tableInfos, result);

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
        // Doris 不支持外键，返回空列表
        return Collections.emptyList();
    }

    @Override
    public List<TableIndexSchema> getIndexes() {
        Map<String, TableSchema> tables = getTables(null, null);
        if (tables == null || tables.isEmpty()) {
            return Collections.emptyList();
        }

        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<TableIndexSchema> result = new ArrayList<>();

        try {
            conn = JdbcUtil.getConnectionForDDL(getDriver(), getUrl(), getDatabaseInfo());

            for (Map.Entry<String, TableSchema> entry : tables.entrySet()) {
                TableSchema table = entry.getValue();
                // 只处理表，不处理视图
                if (!"TABLE".equals(table.getTableType())) {
                    continue;
                }

                try {
                    stat = conn.prepareStatement(String.format(SHOW_INDEX_SQL, entry.getKey()));
                    rs = stat.executeQuery();

                    Map<String, TableIndexSchema> indexMap = new HashMap<>();
                    while (rs.next()) {
                        String indexName = rs.getString("Key_name");
                        String columnName = rs.getString("Column_name");
                        String indexType = rs.getString("Index_type");
                        String properties = rs.getString("Properties");
                        String comment = rs.getString("Comment");

                        TableIndexSchema index = indexMap.get(indexName);
                        if (index == null) {
                            index = new TableIndexSchema();
                            index.setName(indexName);
                            index.setTableName(entry.getKey());
                            index.setUnique(!"1".equals(rs.getString("Non_unique")));
                            index.setType(indexType);
                            index.setProperties(extractProperties(properties));
                            index.setDescription(comment);
                            index.setColumnNames(columnName);
                            indexMap.put(indexName, index);
                        } else {
                            // 复合索引，拼接列名
                            index.setColumnNames(index.getColumnNames() + "," + columnName);
                        }
                    }
                    result.addAll(indexMap.values());
                } catch (Exception e) {
                    log.error("获取表索引信息失败！表名：" + entry.getKey(), e);
                } finally {
                    JdbcUtil.closeResultSet(rs);
                    JdbcUtil.closeStatement(stat);
                }
            }
        } catch (Exception e) {
            log.error("获取Doris数据库索引失败", e);
            throw new BusinessException("获取索引信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

    // ===================== 内部工具方法 =====================

    /**
     * 获取表信息
     */
    private Map<String, TableSchema> getTables(List<String> inTables, List<String> notInTables) {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        Map<String, TableSchema> result = new HashMap<>();
        List<String> params = new ArrayList<>();

        try {
            params.add(getDbName());
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

            // 获取建表语句
            getCreateTableSqls(result);

        } catch (Exception e) {
            log.error("获取Doris数据库表信息失败", e);
            throw new BusinessException("获取表结构信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(stat);
            JdbcUtil.close(conn);
        }
        return result;
    }

    /**
     * 获取建表语句
     */
    private void getCreateTableSqls(Map<String, TableSchema> tables) {
        if (tables == null || tables.isEmpty()) {
            return;
        }

        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;

        try {
            conn = JdbcUtil.getConnectionForDDL(getDriver(), getUrl(), getDatabaseInfo());

            for (Map.Entry<String, TableSchema> entry : tables.entrySet()) {
                try {
                    stat = conn.prepareStatement(String.format(CREATE_TABLE_SQL, entry.getKey()));
                    rs = stat.executeQuery();
                    if (rs.next()) {
                        String tableSql = rs.getString(2); // 第二列是建表语句
                        entry.getValue().setTableSql(tableSql);
                    }
                } catch (Exception e) {
                    log.error("获取建表语句失败！表名：" + entry.getKey(), e);
                } finally {
                    JdbcUtil.closeResultSet(rs);
                    JdbcUtil.closeStatement(stat);
                }
            }
        } catch (Exception e) {
            log.error("获取建表语句失败", e);
        } finally {
            JdbcUtil.closeConnection(conn);
        }
    }

    /**
     * 获取列信息
     */
    private List<TableColumnSchema> getColumnInfos(List<String> inTables, List<String> notInTables) {
        List<String> uniqueConstraintTableList = getUniqueConstraintTableList();

        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<String> params = new ArrayList<>();
        List<TableColumnSchema> result = new ArrayList<>();

        params.add(getDbName());

        try {
            conn = getConnection();

            String sql = buildTableSql(COLUMN_INFO_SQL, inTables, notInTables, params);
            stat = conn.prepareStatement(sql);

            for (int i = 0; i < params.size(); i++) {
                stat.setString(i + 1, params.get(i));
            }

            rs = stat.executeQuery();

            while (rs.next()) {
                TableColumnSchema col = new TableColumnSchema();
                String tableName = rs.getString("TABLE_NAME");
                String columnType = rs.getString("COLUMN_TYPE");
                String dataType = getDataType(columnType);

                col.setTableName(tableName);
                col.setColumnName(rs.getString("COLUMN_NAME"));
                col.setColumnComment(rs.getString("COLUMN_COMMENT"));
                col.setDataType(dataType);
                col.setConstraintType(getConstraintType(rs.getString("COLUMN_KEY"), tableName, uniqueConstraintTableList));
                col.setIsNull(!"NO".equals(rs.getString("IS_NULLABLE")));
                col.setColumnOrder(rs.getInt("ORDINAL_POSITION"));

                // 设置数据长度和精度
                setDataLengthAndPrecision(dataType, col, rs);

                result.add(col);
            }

            return result;
        } catch (Exception e) {
            log.error("获取Doris数据库列信息失败", e);
            throw new BusinessException("获取列信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
    }

    /**
     * 获取含有唯一键约束的表信息
     */
    private List<String> getUniqueConstraintTableList() {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<String> params = new ArrayList<>();
        List<String> result = new ArrayList<>();

        params.add(getDbName());

        try {
            conn = getConnection();
            stat = conn.prepareStatement(UNIQUE_CONSTRAINT_TABLE_SQL);

            for (int i = 0; i < params.size(); i++) {
                stat.setString(i + 1, params.get(i));
            }

            rs = stat.executeQuery();

            while (rs.next()) {
                result.add(rs.getString("TABLE_NAME"));
            }

            return result;
        } catch (Exception e) {
            log.error("获取包含唯一键约束的表信息失败", e);
            throw new BusinessException("获取包含唯一键约束的表信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
    }

    /**
     * 获取约束类型
     */
    private String getConstraintType(String columnKey, String tableName, List<String> uniqueConstraintTableList) {
        if ("UNI".equals(columnKey)) {
            return "P"; // Doris中唯一键作为主键使用
        } else {
            return "C"; // 普通列
        }
    }

    /**
     * 获取数据类型
     * 主要处理Doris中的以下情况：
     * boolean => tinyint(1)
     * string => varchar
     * decimal => decimalv3
     */
    private String getDataType(String columnType) {
        if ("tinyint(1)".equals(columnType)) {
            return "boolean";
        }
        String result = ConnectorUtil.getMsg(columnType);
        // decimalv3 转为 decimal
        if (result != null && result.startsWith("decimal")) {
            return "decimal";
        }
        return result != null ? result : columnType;
    }

    /**
     * 设置数据长度和精度
     */
    private void setDataLengthAndPrecision(String dataType, TableColumnSchema col, ResultSet rs) throws Exception {
        if ("decimal".equals(dataType)) {
            Object dataLengthObj = rs.getObject("NUMERIC_PRECISION");
            if (dataLengthObj != null) {
                col.setDataLength(((Number) dataLengthObj).intValue());
            }
            Object scaleObj = rs.getObject("NUMERIC_SCALE");
            if (scaleObj != null) {
                col.setPrecision(((Number) scaleObj).intValue());
            }
        } else if ("string".equals(dataType)) {
            // string类型无需设置长度
        } else {
            // 其他字符类型，直接获取长度
            Object dataLengthObj = rs.getObject("CHARACTER_MAXIMUM_LENGTH");
            if (dataLengthObj != null) {
                col.setDataLength(rs.getInt("CHARACTER_MAXIMUM_LENGTH"));
            }
        }
    }

    /**
     * 构建表查询SQL
     */
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

    /**
     * 获取表类型（TABLE/VIEW）
     */
    private String getTableType(String tableType) {
        if ("BASE TABLE".equals(tableType)) {
            return "TABLE";
        }
        return "VIEW";
    }

    /**
     * 复制表信息到目标Map
     */
    private void copyToTableSchemaMap(Map<String, TableSchema> src, Map<String, TableSchema> dest) {
        for (Map.Entry<String, TableSchema> entry : src.entrySet()) {
            TableSchema target = new TableSchema();
            BeanUtils.copyProperties(entry.getValue(), target);
            dest.put(entry.getKey(), target);
        }
    }

    /**
     * 提取括号内的内容
     */
    private String extractProperties(String properties) {
        if (properties == null) {
            return null;
        }
        if (propertiesPattern == null) {
            propertiesPattern = Pattern.compile("\\((.*)\\)");
        }
        Matcher matcher = propertiesPattern.matcher(properties);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return properties;
    }
}