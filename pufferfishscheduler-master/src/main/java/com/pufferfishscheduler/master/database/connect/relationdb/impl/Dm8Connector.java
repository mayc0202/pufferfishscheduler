package com.pufferfishscheduler.master.database.connect.relationdb.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * 达梦数据库连接器
 *
 * @author
 * @since 2026-03-30
 */
@Slf4j
public class Dm8Connector extends AbstractDatabaseConnector {

    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";

    /**
     * 表信息
     */
    private static final String TABLE_INFO_SQL = "SELECT TABLE_NAME,TABLE_COMMENT,TABLE_TYPE FROM ("
            + " SELECT A.TABLE_NAME,B.COMMENTS as TABLE_COMMENT,b.table_type FROM ALL_TABLES A,ALL_TAB_COMMENTS B WHERE A.TABLE_NAME=B.TABLE_NAME "
            + "               and A.owner='${schemaName}' and B.owner='${schemaName}' "
            + "              UNION ALL "
            + " SELECT A.VIEW_NAME AS TABLE_NAME,B.COMMENTS AS TABLE_COMMENT,b.table_type FROM ALL_VIEWS A,ALL_TAB_COMMENTS B WHERE A.VIEW_NAME=B.TABLE_NAME"
            + "              and A.owner='${schemaName}' and B.owner='${schemaName}'"
            + " ) WHERE 1=1";

    /**
     * 列信息
     * -- 对于DECIMAL、NUMBER、NUMERIC类型，长度取精度(DATA_PRECISION)
     * -- 对于VARBINARY、BINARY类型，取DATA_LENGTH
     * -- 其他类型，使用CHAR_LENGTH
     * -- time、TIMESTAMP的小数位取DATA_SCALE
     */
    private static final String COLUMN_INFO_SQL =
            "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,COLUMN_ID,DATA_SCALE,NULLABLE,DATA_DEFAULT,COMMENTS, DATA_PRECISION FROM ("
                    + " SELECT A.TABLE_NAME,A.COLUMN_NAME,A.DATA_TYPE," +
                    "         CASE " +
                    "             WHEN A.DATA_TYPE IN ('DECIMAL', 'NUMBER', 'NUMERIC') THEN " +
                    "                 A.DATA_PRECISION " +
                    "             WHEN A.DATA_TYPE IN ('VARBINARY', 'BINARY') THEN " +
                    "                 A.DATA_LENGTH " +
                    "             ELSE " +
                    "                 A.CHAR_LENGTH " +
                    "         END AS DATA_LENGTH," +
                    "A.COLUMN_ID,A.DATA_SCALE,A.NULLABLE,A.DATA_DEFAULT,A.DATA_PRECISION,B.COMMENTS"
                    + " FROM all_tab_columns A,all_col_comments B "
                    + " WHERE A.TABLE_NAME = B.TABLE_NAME  and A.COLUMN_NAME = B.COLUMN_NAME and a.owner='${schemaName}') WHERE 1=1 ";

    /**
     * 查询列主键、唯一键
     */
    private static final String COLUMN_PRI_KEY =
            " SELECT TABLE_NAME,COLUMN_NAME,CONSTRAINT_TYPE FROM "
                    + " (SELECT AU.CONSTRAINT_TYPE, CU.COLUMN_NAME,CU.TABLE_NAME FROM all_CONS_COLUMNS CU,all_CONSTRAINTS AU WHERE CU.CONSTRAINT_NAME = AU.CONSTRAINT_NAME"
                    + " and cu.owner='${schemaName}' and au.owner='${schemaName}') WHERE 1=1 ";

    /**
     * 获取所有外键
     */
    private static final String FK_CONSTRAINT_TABLE_SQL =
            "SELECT UC.CONSTRAINT_NAME, UC.TABLE_NAME, COLS.COLUMN_NAME, RUC.TABLE_NAME AS REFERENCED_TABLE_NAME, RCOLS.COLUMN_NAME AS REFERENCED_COLUMN_NAME " +
                    "FROM USER_CONS_COLUMNS COLS JOIN USER_CONSTRAINTS UC ON COLS.CONSTRAINT_NAME = UC.CONSTRAINT_NAME " +
                    "LEFT JOIN USER_CONSTRAINTS RUC ON UC.R_CONSTRAINT_NAME = RUC.CONSTRAINT_NAME " +
                    "LEFT JOIN USER_CONS_COLUMNS RCOLS ON RUC.CONSTRAINT_NAME = RCOLS.CONSTRAINT_NAME " +
                    "WHERE UC.CONSTRAINT_TYPE = 'R' AND UC.OWNER = ?";

    /**
     * 获取所有索引
     */
    private static final String IDX_CONSTRAINT_TABLE_SQL =
            "SELECT AIC.INDEX_NAME, AIC.TABLE_NAME, " +
                    "LISTAGG(AIC.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY AIC.COLUMN_POSITION) AS COLUMN_NAME, " +
                    "AI.UNIQUENESS, UC.CONSTRAINT_TYPE " +
                    "FROM ALL_IND_COLUMNS AIC " +
                    "JOIN ALL_INDEXES AI ON AIC.INDEX_NAME = AI.INDEX_NAME " +
                    "LEFT JOIN USER_CONSTRAINTS UC ON UC.INDEX_NAME = AIC.INDEX_NAME " +
                    "WHERE AIC.TABLE_OWNER = ? " +
                    "GROUP BY AIC.INDEX_NAME, AIC.TABLE_NAME, AI.UNIQUENESS, UC.CONSTRAINT_TYPE";

    @Override
    public AbstractDatabaseConnector build() {
        this.setDriver(JdbcUrlUtil.getDriver(Constants.DATABASE_TYPE.DM8));
        String url = JdbcUtil.getUrl(Constants.DATABASE_TYPE.DM8, getHost(), String.valueOf(getPort()), getDbName(), getExtConfig());
        this.setUrl(url);
        return this;
    }

    @Override
    public Connection getConnection() {
        try {
            return JdbcUtil.getConnection(getDriver(), getUrl(), getDatabaseInfo());
        } catch (Exception e) {
            log.error("达梦数据库连接创建失败", e);
            throw new BusinessException("创建数据库连接失败！" + e.getMessage());
        }
    }

    @Override
    public Map<String, TableSchema> getTableSchema(List<String> inTableNames, List<String> notInTableNames) {
        Map<String, TableSchema> tableInfos = getTables(inTableNames, notInTableNames);
        List<TableColumnSchema> columnList = getColumnInfos(inTableNames, notInTableNames);

        if (tableInfos.isEmpty() || columnList.isEmpty()) {
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
            log.error("获取达梦数据库外键失败", e);
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
                // 排除主键约束对应的索引
                String constraintType = rs.getString("CONSTRAINT_TYPE");
                if ("P".equals(constraintType)) {
                    continue;
                }

                TableIndexSchema index = new TableIndexSchema();
                index.setName(rs.getString("INDEX_NAME"));
                index.setTableName(rs.getString("TABLE_NAME"));
                index.setUnique("UNIQUE".equals(rs.getString("UNIQUENESS")));
                index.setColumnNames(rs.getString("COLUMN_NAME"));
                result.add(index);
            }
        } catch (Exception e) {
            log.error("获取达梦数据库索引失败", e);
            throw new BusinessException("获取索引信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
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
            conn = getConnection();
            String schema = getSchema();

            String sql = TABLE_INFO_SQL.replace("${schemaName}", schema);
            sql = buildTableSql(sql, inTables, notInTables, params);
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
            log.error("获取达梦数据库表信息失败", e);
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
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<TableColumnSchema> result = new ArrayList<>();
        List<String> params = new ArrayList<>();

        try {
            conn = getConnection();
            Map<String, String> constraintMap = getConstraintTypes(inTables, notInTables);
            String schema = getSchema();

            String sql = COLUMN_INFO_SQL.replace("${schemaName}", schema);
            sql = buildTableSql(sql, inTables, notInTables, params);
            stat = conn.prepareStatement(sql);

            for (int i = 0; i < params.size(); i++) {
                stat.setString(i + 1, params.get(i));
            }

            rs = stat.executeQuery();
            while (rs.next()) {
                TableColumnSchema col = new TableColumnSchema();
                col.setTableName(rs.getString("TABLE_NAME"));
                col.setColumnName(rs.getString("COLUMN_NAME"));
                col.setColumnComment(rs.getString("COMMENTS"));
                col.setDataType(rs.getString("DATA_TYPE"));
                col.setIsNull(!"N".equals(rs.getString("NULLABLE")));
                col.setColumnOrder(rs.getInt("COLUMN_ID"));

                // 处理数据长度
                String dataLength = rs.getString("DATA_LENGTH");
                if (StringUtils.isNotBlank(dataLength)) {
                    col.setDataLength(Integer.parseInt(dataLength));
                }
                // 单独处理NUMBER类型
                if ("NUMBER".equals(col.getDataType())) {
                    String precision = rs.getString("DATA_PRECISION");
                    if (StringUtils.isNotBlank(precision)) {
                        col.setDataLength(Integer.parseInt(precision));
                    }
                }
                // 处理TIMESTAMP类型
                if (col.getDataType() != null && col.getDataType().startsWith("TIMESTAMP")) {
                    col.setDataType("TIMESTAMP");
                }

                // 处理精度
                Object scaleObj = rs.getObject("DATA_SCALE");
                if (scaleObj != null) {
                    col.setPrecision(((BigDecimal) scaleObj).intValue());
                }

                // 设置约束类型
                col.setConstraintType("C");
                String key = col.getTableName() + "-" + col.getColumnName();
                if (constraintMap.containsKey(key)) {
                    col.setConstraintType(constraintMap.get(key));
                }

                result.add(col);
            }
        } catch (Exception e) {
            log.error("获取达梦数据库列信息失败", e);
            throw new BusinessException("获取列信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

    /**
     * 获取约束类型（主键、唯一键）
     */
    private Map<String, String> getConstraintTypes(List<String> inTables, List<String> notInTables) {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        Map<String, String> result = new HashMap<>();
        List<String> params = new ArrayList<>();

        try {
            conn = getConnection();
            String schema = getSchema();
            String sql = COLUMN_PRI_KEY.replace("${schemaName}", schema);
            sql = buildTableSql(sql, inTables, notInTables, params);
            stat = conn.prepareStatement(sql);

            for (int i = 0; i < params.size(); i++) {
                stat.setString(i + 1, params.get(i));
            }

            rs = stat.executeQuery();
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                String constraintType = rs.getString("CONSTRAINT_TYPE");

                String key = tableName + "-" + columnName;
                if ("P".equals(constraintType)) {
                    result.put(key, "P"); // 主键
                } else if ("U".equals(constraintType)) {
                    result.put(key, "U"); // 唯一键
                }
            }
        } catch (Exception e) {
            log.error("获取达梦数据库约束信息失败", e);
            throw new BusinessException("获取列约束信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
        }
        return result;
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
    private String getTableType(String type) {
        if ("TABLE".equalsIgnoreCase(type) || "BASE TABLE".equalsIgnoreCase(type)) {
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
     * 获取数据量
     */
    public Long getDataVolumeByTableName(String tableName, Connection conn) {
        PreparedStatement stat = null;
        ResultSet rs = null;
        long result = 0L;

        try {
            String schema = getSchema();

            String tableNameWithSchema = String.format("\"%s\".\"%s\"", schema, tableName);
            String sql = "SELECT COUNT(1) FROM " + tableNameWithSchema;
            stat = conn.prepareStatement(sql);
            rs = stat.executeQuery();
            if (rs.next()) {
                result = rs.getLong(1);
            }
        } catch (Exception e) {
            log.error(String.format("获取表数据量失败！表名：%s，原因：%s", tableName, e.getMessage()), e);
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(stat);
        }
        return result;
    }
}