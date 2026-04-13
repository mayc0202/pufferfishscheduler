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
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Oracle数据库连接器
 *
 * @author Mayc
 * @since 2026-03-16  16:49
 */
@Slf4j
public class OracleConnector extends AbstractDatabaseConnector {

    private static String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final String SERVICE_URL_FORMAT = "jdbc:oracle:thin:@//%s:%s/%s";
    private static final String SID_URL_FORMAT = "jdbc:oracle:thin:@%s:%s:%s";

    private static final String TABLE_INFO_SQL = "SELECT TABLE_NAME,TABLE_COMMENT,TABLE_TYPE FROM ("
            + " SELECT A.TABLE_NAME,B.COMMENTS as TABLE_COMMENT,b.table_type FROM ALL_TABLES A,ALL_TAB_COMMENTS B WHERE A.TABLE_NAME=B.TABLE_NAME "
            + " 					 and A.owner='${schemaName}' and B.owner='${schemaName}' "
            + " 					UNION ALL "
            + " SELECT A.VIEW_NAME AS TABLE_NAME,B.COMMENTS AS TABLE_COMMENT,b.table_type FROM ALL_VIEWS A,ALL_TAB_COMMENTS B WHERE A.VIEW_NAME=B.TABLE_NAME"
            + " 					and A.owner='${schemaName}' and B.owner='${schemaName}'"
            + "                 UNION ALL "
            + " SELECT A.TABLE_NAME ,B.COMMENTS AS TABLE_COMMENT,'VIEW' as table_type FROM ALL_TABLES A,ALL_MVIEW_COMMENTS B WHERE A.TABLE_NAME=B.MVIEW_NAME"
            + "                 and A.owner='${schemaName}' and B.owner='${schemaName}'"
            + " ) WHERE 1=1";


    private static final String COLUMN_INFO_SQL =
            "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,COLUMN_ID,DATA_SCALE,NULLABLE,DATA_DEFAULT,COMMENTS, DATA_PRECISION FROM ("
                    + " SELECT A.TABLE_NAME,A.COLUMN_NAME,A.DATA_TYPE," +
                    "CASE " +
                    " WHEN DATA_TYPE IN ('NUMBER', 'FLOAT') AND DATA_PRECISION IS NOT NULL THEN " +
                    "  TO_CHAR(DATA_PRECISION) " +
                    " WHEN DATA_TYPE IN ('CHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2', 'RAW') THEN " +
                    "  TO_CHAR(CASE WHEN CHAR_LENGTH = 0 THEN DATA_LENGTH ELSE CHAR_LENGTH END) " +
                    " ELSE NULL " +
                    "END AS DATA_LENGTH," +
                    "A.COLUMN_ID,A.DATA_SCALE,A.NULLABLE,A.DATA_DEFAULT,A.DATA_PRECISION,B.COMMENTS"
                    + " FROM all_tab_columns A,all_col_comments B "
                    + " WHERE A.TABLE_NAME = B.TABLE_NAME  and A.COLUMN_NAME = B.COLUMN_NAME and a.owner='${schemaName}' and b.owner='${schemaName}') WHERE 1=1 ";


    private static final String COLUMN_PRI_KEY =
            " SELECT TABLE_NAME,COLUMN_NAME,CONSTRAINT_TYPE FROM "
                    + " (SELECT AU.CONSTRAINT_TYPE, CU.COLUMN_NAME,CU.TABLE_NAME FROM all_CONS_COLUMNS CU,all_CONSTRAINTS AU WHERE CU.CONSTRAINT_NAME = AU.CONSTRAINT_NAME"
                    + " and cu.owner='${schemaName}' and au.owner='${schemaName}') WHERE 1=1 ";

    private static final String FK_CONSTRAINT_TABLE_SQL =
            "SELECT UC.CONSTRAINT_NAME, UC.TABLE_NAME, COLS.COLUMN_NAME, RUC.TABLE_NAME AS REFERENCED_TABLE_NAME, RCOLS.COLUMN_NAME AS REFERENCED_COLUMN_NAME " +
                    "FROM USER_CONS_COLUMNS COLS JOIN USER_CONSTRAINTS UC ON COLS.CONSTRAINT_NAME = UC.CONSTRAINT_NAME " +
                    "LEFT JOIN USER_CONSTRAINTS RUC ON UC.R_CONSTRAINT_NAME = RUC.CONSTRAINT_NAME " +
                    "LEFT JOIN USER_CONS_COLUMNS RCOLS ON RUC.CONSTRAINT_NAME = RCOLS.CONSTRAINT_NAME " +
                    "WHERE UC.CONSTRAINT_TYPE = 'R' AND UC.OWNER = ?";

    private static final String IDX_CONSTRAINT_TABLE_SQL =
            "SELECT AIC.INDEX_NAME, AIC.TABLE_NAME, " +
                    "LISTAGG(AIC.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY AIC.COLUMN_NAME) AS COLUMN_NAME, " +
                    "AI.UNIQUENESS, UC.CONSTRAINT_TYPE " +
                    "FROM ALL_IND_COLUMNS AIC " +
                    "JOIN ALL_INDEXES AI ON AIC.INDEX_NAME = AI.INDEX_NAME " +
                    "LEFT JOIN USER_CONSTRAINTS UC ON UC.INDEX_NAME = AIC.INDEX_NAME " +
                    "WHERE AIC.TABLE_OWNER = ? " +
                    "GROUP BY AIC.INDEX_NAME, AIC.TABLE_NAME, AI.UNIQUENESS, UC.CONSTRAINT_TYPE";

    @Override
    public AbstractDatabaseConnector build() {
        this.setDriver(DRIVER_CLASS);
        String url = JdbcUtil.getUrl(Constants.DATABASE_TYPE.ORACLE, getHost(), String.valueOf(getPort()), getDbName(), getExtConfig());
        this.setUrl(url);
        return this;
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
    public Connection getConnection() {
        try {
            return JdbcUtil.getConnection(getDriver(), getUrl(), getDatabaseInfo());
        } catch (Exception e) {
            log.error("Oracle连接创建失败", e);
            throw new BusinessException("创建数据库连接失败！" + e.getMessage());
        }
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
            log.error("获取Oracle外键失败", e);
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
                index.setUnique("UNIQUE".equals(rs.getString("UNIQUENESS")));
                index.setColumnNames(rs.getString("COLUMN_NAME"));
                result.add(index);
            }
        } catch (Exception e) {
            log.error("获取Oracle索引失败", e);
            throw new BusinessException("获取索引信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
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
            log.error("获取Oracle表信息失败", e);
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

                // 长度
                String lenStr = rs.getString("DATA_LENGTH");
                if (StringUtils.isNotBlank(lenStr)) {
                    col.setDataLength(Integer.parseInt(lenStr));
                }
                if ("NUMBER".equals(col.getDataType())) {
                    String precision = rs.getString("DATA_PRECISION");
                    if (StringUtils.isNotBlank(precision)) {
                        col.setDataLength(Integer.parseInt(precision));
                    }
                }
                if (col.getDataType().startsWith("TIMESTAMP")) {
                    col.setDataType("TIMESTAMP");
                }

                // 精度
                Object scaleObj = rs.getObject("DATA_SCALE");
                if (scaleObj != null) {
                    col.setPrecision(((BigDecimal) scaleObj).intValue());
                }

                // 约束
                col.setConstraintType("C");
                String key = col.getTableName() + "-" + col.getColumnName();
                if (constraintMap.containsKey(key)) {
                    col.setConstraintType(constraintMap.get(key));
                }

                result.add(col);
            }
        } catch (Exception e) {
            log.error("获取Oracle列信息失败", e);
            throw new BusinessException("获取列信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
        return result;
    }

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
                String table = rs.getString("TABLE_NAME");
                String col = rs.getString("COLUMN_NAME");
                String type = rs.getString("CONSTRAINT_TYPE");
                if ("P".equals(type)) {
                    result.put(table + "-" + col, "P");
                } else if ("U".equals(type)) {
                    result.put(table + "-" + col, "U");
                }
            }
        } catch (Exception e) {
            log.error("获取Oracle约束失败", e);
            throw new BusinessException("获取列约束信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
        }
        return result;
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

    private String getTableType(String type) {
        return "TABLE".equalsIgnoreCase(type) ? "TABLE" : "VIEW";
    }

    private void copyToDBTableMap(Map<String, TableSchema> src, Map<String, TableSchema> dest) {
        for (Map.Entry<String, TableSchema> entry : src.entrySet()) {
            TableSchema target = new TableSchema();
            BeanUtils.copyProperties(entry.getValue(), target);
            dest.put(entry.getKey(), target);
        }
    }
}