package com.pufferfishscheduler.service.database.db.connector.relationdb.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.ConnectorUtil;
import com.pufferfishscheduler.service.database.db.connector.relationdb.AbstractDatabaseConnector;
import com.pufferfishscheduler.domain.domain.TableColumnSchema;
import com.pufferfishscheduler.domain.domain.TableForeignKey;
import com.pufferfishscheduler.domain.domain.TableIndexSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * MySQL连接器
 */
@Slf4j
public class MySQLConnector extends AbstractDatabaseConnector {

    private static final String URL_FORMAT = "jdbc:mysql://%s:%s/%s?useCursorFetch=true&useSSL=false&useUnicode=yes&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&serverTimezone=Asia/Shanghai";

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
     * 获取含有主键约束的表信息
     */
    private static final String PRIMARY_CONSTRAINT_TABLE_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE  CONSTRAINT_TYPE = 'PRIMARY KEY' AND TABLE_SCHEMA = ?";


    /**
     * 获取所有外键
     */
    private static final String FK_CONSTRAINT_TABLE_SQL = "SELECT b.CONSTRAINT_NAME, b.TABLE_NAME, b.REFERENCED_TABLE_NAME, GROUP_CONCAT(b.COLUMN_NAME ORDER BY b.ORDINAL_POSITION) as COLUMN_NAME, GROUP_CONCAT(b.REFERENCED_COLUMN_NAME order by b.ORDINAL_POSITION) as REFERENCED_COLUMN_NAME " +
            "FROM information_schema.TABLE_CONSTRAINTS a, INFORMATION_SCHEMA.KEY_COLUMN_USAGE b " +
            "WHERE a.CONSTRAINT_NAME = b.CONSTRAINT_NAME AND a.CONSTRAINT_TYPE = 'FOREIGN KEY' AND a.CONSTRAINT_SCHEMA = ? " +
            " group by b.CONSTRAINT_NAME, b.TABLE_NAME, b.REFERENCED_TABLE_NAME";

    /**
     * 获取所有索引
     */
    private static final String IDX_CONSTRAINT_TABLE_SQL = "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as COLUMN_NAME " +
            "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND INDEX_NAME != 'PRIMARY' " +
            "GROUP BY TABLE_NAME,INDEX_NAME,NON_UNIQUE";


    /**
     * 构造连接器
     *
     * @return
     */
    @Override
    public AbstractDatabaseConnector build() {
        this.setDriver(JdbcUtil.getDriver(Constants.DATABASE_TYPE.MYSQL));
        this.setUrl(JdbcUtil.getUrl(Constants.DATABASE_TYPE.MYSQL, getHost(), String.valueOf(getPort()), getDbName(), getExtConfig()));
        return this;
    }

    /**
     * 获取数据源连接
     *
     * @return
     */
    @Override
    public Connection getConnection() {
        Connection conn;
        try {
            conn = JdbcUtil.getConnection(getDriver(), getUrl(), getDatabaseInfo());
        } catch (Exception e) {
            log.error(null, e);
            throw new BusinessException("创建数据库连接失败！" + e.getMessage());
        }

        return conn;
    }

    @Override
    public List<TableForeignKey> getForeignKeys() {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;

        List<String> params = new ArrayList<>();
        List<TableForeignKey> result = new ArrayList<>();

        params.add(getDbName());

        try {
            conn = getConnection();
            stat = conn.prepareStatement(FK_CONSTRAINT_TABLE_SQL);
            int iLoop = 1;
            for (String param : params) {
                stat.setString(iLoop, param);
                iLoop++;
            }
            rs = stat.executeQuery();

            while (rs.next()) {
                TableForeignKey tableForeignKey = new TableForeignKey();
                tableForeignKey.setName(rs.getString("CONSTRAINT_NAME"));
                tableForeignKey.setTableName(rs.getString("TABLE_NAME"));
                tableForeignKey.setRefTableName(rs.getString("REFERENCED_TABLE_NAME"));
                tableForeignKey.setColumnNames(rs.getString("COLUMN_NAME"));
                tableForeignKey.setRefColumnNames(rs.getString("REFERENCED_COLUMN_NAME"));
                result.add(tableForeignKey);
            }

            return result;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("获取外键信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
    }

    /**
     * 获取索引
     *
     * @return
     */
    @Override
    public List<TableIndexSchema> getIndexes() {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;

        List<String> params = new ArrayList<>();
        List<TableIndexSchema> result = new ArrayList<>();

        params.add(getDbName());

        try {
            conn = getConnection();
            stat = conn.prepareStatement(IDX_CONSTRAINT_TABLE_SQL);
            int iLoop = 1;
            for (String param : params) {
                stat.setString(iLoop, param);
                iLoop++;
            }
            rs = stat.executeQuery();

            while (rs.next()) {
                TableIndexSchema tableIndexSchema = new TableIndexSchema();
                tableIndexSchema.setName(rs.getString("INDEX_NAME"));
                tableIndexSchema.setTableName(rs.getString("TABLE_NAME"));
                tableIndexSchema.setUnique(!rs.getBoolean("NON_UNIQUE"));
                tableIndexSchema.setColumnNames(rs.getString("COLUMN_NAME"));
                result.add(tableIndexSchema);
            }

            return result;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("获取索引信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
    }

    /**
     * 获取表结构信息
     *
     * @param inTables    仅包含 inTables 指定的表
     * @param notInTables 排除 notInTables 包含的表
     * @return
     */
    @Override
    public Map<String, TableSchema> getTableSchema(List<String> inTables, List<String> notInTables) {
        Map<String, TableSchema> tableInfos = getTables(inTables, notInTables);
        List<TableColumnSchema> tableTableColumnSchemas = getColumnInfos(inTables, notInTables);

        if (tableInfos.size() == 0) {
            return null;
        }

        if (tableTableColumnSchemas == null || tableTableColumnSchemas.size() == 0) {
            return null;
        }


        Map<String, TableSchema> tableWichColumnInfos = new HashMap<>();
        copyToDBTableWithColumnMap(tableInfos, tableWichColumnInfos);


        for (TableColumnSchema tableColumnSchema : tableTableColumnSchemas) {
            TableSchema tableSchema = tableWichColumnInfos.get(tableColumnSchema.getTableName());
            if (tableSchema == null) {
                continue;
            }

            Map<String, TableColumnSchema> columnInfos = tableSchema.getColumnInfos();
            if (columnInfos == null) {
                columnInfos = new HashMap<>();
            }

            columnInfos.put(tableColumnSchema.getColumnName(), tableColumnSchema);
            tableSchema.setColumnInfos(columnInfos);
        }

        return tableWichColumnInfos;
    }

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

            int iLoop = 1;
            for (String param : params) {
                stat.setString(iLoop, param);
                iLoop++;
            }

            rs = stat.executeQuery();

            while (rs.next()) {
                TableSchema tableSchema = new TableSchema();
                tableSchema.setTableName(rs.getString("TABLE_NAME"));
                tableSchema.setTableComment(rs.getString("TABLE_COMMENT"));
                tableSchema.setTableType(rs.getString("TABLE_TYPE"));
                result.put(tableSchema.getTableName(), tableSchema);
            }

        } catch (Exception e) {
            log.error(null, e);
            throw new BusinessException("获取表结构信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(stat);
            JdbcUtil.close(conn);
        }

        return result;
    }

    private List<TableColumnSchema> getColumnInfos(List<String> inTables,
                                                   List<String> notInTables) {

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

            int iLoop = 1;
            for (String param : params) {
                stat.setString(iLoop, param);
                iLoop++;
            }

            rs = stat.executeQuery();

            while (rs.next()) {
                TableColumnSchema tableColumnSchema = new TableColumnSchema();
                String tableName = rs.getString("TABLE_NAME");
                tableColumnSchema.setColumnName(rs.getString("COLUMN_NAME"));
                tableColumnSchema.setColumnComment(rs.getString("COLUMN_COMMENT"));
                tableColumnSchema.setDataType(rs.getString("DATA_TYPE"));
                tableColumnSchema.setConstraintType(getConstraintType(rs.getString("COLUMN_KEY"), tableName, primaryConstraintTableList));
                Object precesionObj = rs.getObject("NUMERIC_SCALE");
                if (precesionObj != null) {
                    //对应bigint UNSIGNED类型
                    if (precesionObj instanceof BigInteger) {
                        if (((BigInteger) precesionObj).intValue() != 0) {
                            tableColumnSchema.setPrecision(((BigInteger) precesionObj).intValue());
                        }
                    }
                    //对应bigint类型
                    if (precesionObj instanceof Long) {
                        if (((Long) precesionObj).intValue() != 0) {
                            tableColumnSchema.setPrecision(((Long) precesionObj).intValue());
                        }
                    }
                }
                String dataLength = ConnectorUtil.getMsg(rs.getString("COLUMN_TYPE"));
                if (StringUtils.isNotBlank(dataLength)) {
                    tableColumnSchema.setDataLength(Integer.valueOf(dataLength));
                }
                tableColumnSchema.setIsNull(!"NO".equals(rs.getString("IS_NULLABLE")));
                tableColumnSchema.setTableName(tableName);
                tableColumnSchema.setColumnOrder(Integer.valueOf(rs.getString("ORDINAL_POSITION")));
                result.add(tableColumnSchema);

            }

            return result;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("获取列信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }

    }

    /**
     * 获取含有主键约束的表信息
     *
     * @return
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

            int iLoop = 1;
            for (String param : params) {
                stat.setString(iLoop, param);
                iLoop++;
            }

            rs = stat.executeQuery();

            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                result.add(tableName);

            }

            return result;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("获取包含主键约束的表信息失败！" + e.getMessage());
        } finally {
            JdbcUtil.closeResultSet(rs);
            JdbcUtil.closeStatement(stat);
            JdbcUtil.closeConnection(conn);
        }
    }

    private void copyToDBTableWithColumnMap(Map<String, TableSchema> src, Map<String, TableSchema> dest) {
        for (Map.Entry<String, TableSchema> entry : src.entrySet()) {
            TableSchema dbTableWithColumnInfo = new TableSchema();
            BeanUtils.copyProperties(entry.getValue(), dbTableWithColumnInfo);

            dest.put(entry.getKey(), dbTableWithColumnInfo);
        }
    }


    /**
     * 构建表sql
     *
     * @param sql
     * @param inTables
     * @param notInTables
     * @param params
     * @return
     */
    private String buildTableSql(String sql, List<String> inTables, List<String> notInTables, List<String> params) {

        //查询指定的表
        if (inTables != null && inTables.size() > 0) {
            StringBuilder tableCondition = new StringBuilder();
            sql = sql + " and TABLE_NAME in (";
            for (int i = 0; i < inTables.size(); i++) {
                tableCondition.append("?");
                if (i < inTables.size() - 1) {
                    tableCondition.append(",");
                }
                params.add(inTables.get(i));
            }
            sql = sql + tableCondition + ")";
        }

        //排查指定的表
        if (notInTables != null && notInTables.size() > 0) {
            StringBuilder tableCondition = new StringBuilder();
            sql = sql + " and TABLE_NAME not in (";
            for (int i = 0; i < notInTables.size(); i++) {
                tableCondition.append("?");
                if (i < notInTables.size() - 1) {
                    tableCondition.append(",");
                }
                params.add(notInTables.get(i));
            }
            sql = sql + tableCondition + ")";
        }
        return sql;
    }

    private String getConstraintType(String columnKey, String tableName, List<String> primaryConstraintTableList) {
        if ("PRI".equals(columnKey)) {
            //继续判断表是否包含主键，因为存在一种情况：如果表没有主键，但是该字段是非空唯一索引，那么COLUMN_KEY也可能显示为PRI
            if (primaryConstraintTableList != null && primaryConstraintTableList.contains(tableName)) {
                return "P";//主键
            } else {
                return "U";
            }
        } else if ("MUI".equals(columnKey)) {
            return "R";//外键
        } else if ("UNI".equals(columnKey)) {
            return "U";//唯一键
        } else {
            return "C";//普通列
        }
    }

    /**
     * 获取表类型
     *
     * @param tableType
     * @return
     */
    private String getTableType(String tableType) {
        if ("BASE TABLE".equals(tableType)) {
            return "TABLE";
        }
        return "VIEW";
    }
}
