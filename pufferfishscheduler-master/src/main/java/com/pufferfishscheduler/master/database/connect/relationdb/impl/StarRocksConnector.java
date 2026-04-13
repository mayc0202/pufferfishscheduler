package com.pufferfishscheduler.master.database.connect.relationdb.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.ConnectorUtil;
import com.pufferfishscheduler.common.utils.JdbcUrlUtil;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.domain.domain.TableColumnSchema;
import com.pufferfishscheduler.domain.domain.TableForeignKey;
import com.pufferfishscheduler.domain.domain.TableIndexSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.master.database.connect.relationdb.AbstractDatabaseConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * StarRocks数据库连接器
 *
 * @author
 * @since 2026-03-30
 */
@Slf4j
public class StarRocksConnector extends AbstractDatabaseConnector {

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
     * 获取含有主键约束的表信息（StarRocks主键模型）
     */
    private static final String PRIMARY_CONSTRAINT_TABLE_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES_CONFIG WHERE TABLE_MODEL = 'PRIMARY_KEYS' AND TABLE_SCHEMA = ?";

    @Override
    public AbstractDatabaseConnector build() {
        this.setDriver(JdbcUrlUtil.getDriver(Constants.DATABASE_TYPE.STAR_ROCKS));
        String url = JdbcUtil.getUrl(Constants.DATABASE_TYPE.STAR_ROCKS, getHost(), String.valueOf(getPort()), getDbName(), getExtConfig());
        this.setUrl(url);
        return this;
    }

    @Override
    public Connection getConnection() {
        try {
            return JdbcUtil.getConnection(getDriver(), getUrl(), getDatabaseInfo());
        } catch (Exception e) {
            log.error("StarRocks数据库连接创建失败", e);
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
        // StarRocks 不支持外键，返回空列表
        return Collections.emptyList();
    }

    @Override
    public List<TableIndexSchema> getIndexes() {
        // StarRocks 索引信息可通过其他方式获取，暂不实现
        return Collections.emptyList();
    }

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

        } catch (Exception e) {
            log.error("获取StarRocks数据库表信息失败", e);
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
        List<String> primaryConstraintTableList = getPrimaryConstraintTableList();

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
                col.setConstraintType(getConstraintType(rs.getString("COLUMN_KEY"), tableName, primaryConstraintTableList));
                col.setIsNull(!"NO".equals(rs.getString("IS_NULLABLE")));
                col.setColumnOrder(rs.getInt("ORDINAL_POSITION"));

                // 设置精度
                Object scaleObj = rs.getObject("NUMERIC_SCALE");
                if (scaleObj != null) {
                    if (scaleObj instanceof BigInteger) {
                        col.setPrecision(((BigInteger) scaleObj).intValue());
                    } else if (scaleObj instanceof Long) {
                        col.setPrecision(((Long) scaleObj).intValue());
                    }
                }

                // 设置数据长度
                String dataLength = ConnectorUtil.getMsg(columnType);
                if (StringUtils.isNotBlank(dataLength)) {
                    col.setDataLength(Integer.valueOf(dataLength));
                }

                result.add(col);
            }

            return result;
        } catch (Exception e) {
            log.error("获取StarRocks数据库列信息失败", e);
            throw new BusinessException("获取列信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
    }

    /**
     * 获取含有主键约束的表信息
     */
    private List<String> getPrimaryConstraintTableList() {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<String> params = new ArrayList<>();
        List<String> result = new ArrayList<>();

        params.add(getDbName());

        try {
            conn = getConnection();
            stat = conn.prepareStatement(PRIMARY_CONSTRAINT_TABLE_SQL);

            for (int i = 0; i < params.size(); i++) {
                stat.setString(i + 1, params.get(i));
            }

            rs = stat.executeQuery();

            while (rs.next()) {
                result.add(rs.getString("TABLE_NAME"));
            }

            return result;
        } catch (Exception e) {
            log.error("获取包含主键约束的表信息失败", e);
            throw new BusinessException("获取包含主键约束的表信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
    }

    /**
     * 获取约束类型
     */
    private String getConstraintType(String columnKey, String tableName, List<String> primaryConstraintTableList) {
        if ("PRI".equals(columnKey)) {
            // 继续判断表是否包含主键，因为存在一种情况：如果表没有主键，但是该字段是非空唯一索引，那么COLUMN_KEY也可能显示为PRI
            if (primaryConstraintTableList != null && primaryConstraintTableList.contains(tableName)) {
                return "P"; // 主键
            } else {
                return "U"; // 唯一键
            }
        } else if ("MUI".equals(columnKey)) {
            return "R"; // 外键
        } else if ("UNI".equals(columnKey)) {
            return "U"; // 唯一键
        } else {
            return "C"; // 普通列
        }
    }

    /**
     * 获取数据类型
     */
    private String getDataType(String columnType) {
        String result = ConnectorUtil.getMsg(columnType);
        return result != null ? result : columnType;
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
}