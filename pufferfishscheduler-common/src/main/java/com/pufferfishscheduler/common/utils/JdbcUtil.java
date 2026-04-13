package com.pufferfishscheduler.common.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.domain.ColumnMetaData;
import com.pufferfishscheduler.domain.domain.QueryResult;
import com.pufferfishscheduler.domain.domain.TableMetaData;
import com.pufferfishscheduler.domain.model.database.DBConnectionInfo;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Collections;


/**
 * JDBC工具类
 */
public class JdbcUtil {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtil.class);

    // URL格式常量
    private static final String MYSQL_URL_FORMAT = "jdbc:mysql://%s:%s/%s";
    private static final String ORACLE_SERVICE_URL_FORMAT = "jdbc:oracle:thin:@//%s:%s/%s";
    private static final String ORACLE_SID_URL_FORMAT = "jdbc:oracle:thin:@%s:%s:%s";
    private static final String POSTGRESQL_URL_FORMAT = "jdbc:postgresql://%s:%s/%s";
    private static final String SQLSERVER_URL_FORMAT = "jdbc:sqlserver://%s:%s;DatabaseName=%s";
    private static final String DM_URL_FORMAT = "jdbc:dm://%s:%s/%s";

    // 驱动类名常量
    // MySQL 8.x 推荐驱动类（旧的 com.mysql.jdbc.Driver 已废弃）
    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";
    private static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String DM_DRIVER = "dm.jdbc.driver.DmDriver";

    // 连接超时时间（秒）
    private static final int LOGIN_TIMEOUT = 10;


    /**
     * 获取URL
     *
     * @param type
     * @param host
     * @param port
     * @param dbName
     * @param extConfig
     * @return
     */
    public static String getUrl(String type, String host, String port, String dbName, String extConfig) {
        switch (type) {
            case Constants.DATABASE_TYPE.MYSQL:
            case Constants.DATABASE_TYPE.DORIS:
            case Constants.DATABASE_TYPE.STAR_ROCKS:
                return String.format(MYSQL_URL_FORMAT, host, port, dbName);

            case Constants.DATABASE_TYPE.ORACLE:
                JSONObject jsonObject = JSON.parseObject(extConfig);
                String connectType = jsonObject.getString(Constants.DATABASE_EXT_CONFIG.ORACLE_CONNECT_TYPE);
                return Constants.ORACLE_CONNECT_TYPE.SID.equals(connectType)
                        ? String.format(ORACLE_SID_URL_FORMAT, host, port, dbName)
                        : String.format(ORACLE_SERVICE_URL_FORMAT, host, port, dbName);

            case Constants.DATABASE_TYPE.POSTGRESQL:
                return String.format(POSTGRESQL_URL_FORMAT, host, port, dbName);

            case Constants.DATABASE_TYPE.SQL_SERVER:
                return String.format(SQLSERVER_URL_FORMAT, host, port, dbName);

            case Constants.DATABASE_TYPE.DM8:
                return String.format(DM_URL_FORMAT, host, port, dbName);

            default:
                throw new IllegalArgumentException("Unsupported database type: " + type);
        }
    }

    /**
     * 获取Driver
     *
     * @param type
     * @return
     */
    public static String getDriver(String type) {
        return switch (type) {
            case Constants.DATABASE_TYPE.MYSQL,
                 Constants.DATABASE_TYPE.DORIS,
                 Constants.DATABASE_TYPE.STAR_ROCKS
                    -> MYSQL_DRIVER;
            case Constants.DATABASE_TYPE.ORACLE
                    -> ORACLE_DRIVER;
            case Constants.DATABASE_TYPE.POSTGRESQL
                    -> POSTGRESQL_DRIVER;
            case Constants.DATABASE_TYPE.SQL_SERVER
                    -> SQLSERVER_DRIVER;
            case Constants.DATABASE_TYPE.DM8
                    -> DM_DRIVER;
            default -> throw new BusinessException(String.format("暂不支持[%s]此类型数据库!", type));
        };
    }

    /**
     * 获取数据库连接（基本方式）
     */
    public static Connection getConnection(String driverName, String url, String user, String password) {
        return getConnection(driverName, url, user, password, null);
    }

    /**
     * 获取数据库连接（带schema）
     */
    public static Connection getConnection(String driverName, String url, String user, String password, String schema) {
        Properties properties = new Properties();
        properties.put("user", user);
        properties.put("password", password);
        if (StringUtils.isNotBlank(schema)) {
            properties.put("schema", schema);
        }
        return createConnection(driverName, url, properties);
    }

    /**
     * 获取数据库连接（使用DatabaseVo）
     */
    public static Connection getConnection(String driverName, String url, DBConnectionInfo databaseInfo) {
        return createConnection(driverName, url, buildConnectionProperties(databaseInfo, true));
    }

    /**
     * 获取数据库连接（用于DDL操作）
     */
    public static Connection getConnectionForDDL(String driverName, String url, DBConnectionInfo databaseInfo) {
        Properties properties = buildConnectionProperties(databaseInfo, false);
        properties.remove("useCursorFetch"); // DDL操作不需要游标获取
        return createConnection(driverName, url, properties);
    }

    /**
     * 创建数据库连接（核心方法）
     */
    private static Connection createConnection(String driverName, String url, Properties properties) {
        try {
            Class.forName(driverName);
            DriverManager.setLoginTimeout(LOGIN_TIMEOUT);
            return DriverManager.getConnection(url, properties);
        } catch (ClassNotFoundException e) {
            String errorMsg = "JDBC driver not found: " + driverName;
            logger.error(errorMsg, e);
            throw new BusinessException(errorMsg);
        } catch (SQLException e) {
            String errorMsg = "Failed to get database connection to: " + url;
            logger.error(errorMsg, e);
            throw new BusinessException(errorMsg);
        }
    }

    /**
     * 构建连接属性
     */
    private static Properties buildConnectionProperties(DBConnectionInfo databaseInfo, boolean useCursorFetch) {
        Properties properties = new Properties();
        properties.put("user", databaseInfo.getUsername());
        properties.put("password", databaseInfo.getPassword());

        // 设置schema
        if (StringUtils.isNotBlank(databaseInfo.getDbSchema())) {
            String schemaKey = Constants.DATABASE_TYPE.POSTGRESQL.equals(databaseInfo.getType())
                    ? "currentSchema" : "schema";
            properties.setProperty(schemaKey, databaseInfo.getDbSchema());
        }

        // MySQL类型数据库的特殊配置
        if (Constants.DATABASE_TYPE.MYSQL.equals(databaseInfo.getType())
                || Constants.DATABASE_TYPE.DORIS.equals(databaseInfo.getType())) {
            addMysqlDefaultProperties(properties, useCursorFetch);
        }

        // 添加额外配置属性
        if (StringUtils.isNotBlank(databaseInfo.getProperties())) {
            Map<String, String> extraProps = JSON.parseObject(databaseInfo.getProperties(), HashMap.class);
            properties.putAll(extraProps);
        }

        return properties;
    }

    /**
     * 添加MySQL默认连接属性
     */
    private static void addMysqlDefaultProperties(Properties properties, boolean useCursorFetch) {
        properties.put("useSSL", "false");
        // MySQL 8 + caching_sha2_password 常见必需项，否则会报 “Public Key Retrieval is not allowed”
        properties.put("allowPublicKeyRetrieval", "true");
        properties.put("useUnicode", "yes");
        properties.put("characterEncoding", "UTF-8");
        properties.put("zeroDateTimeBehavior", "convertToNull");
        properties.put("allowMultiQueries", "true");
        properties.put("serverTimezone", "Asia/Shanghai");
        properties.put("useOldAliasMetadataBehavior", "true");

        if (useCursorFetch) {
            properties.put("useCursorFetch", "true");
        }
    }


    public static PreparedStatement preparedStatement(Connection connection, String sql) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            return preparedStatement;
        } catch (SQLException e) {
            logger.error("获取PreparedStatement失败！", e);
            throw new BusinessException("获取PreparedStatement失败！");
        }
    }

    /**
     * 关闭 Connection
     *
     * @param conn
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("关闭 jdbc Connection 失败！", e);
            }
        }
    }

    /**
     * 关闭 Statement
     *
     * @param state
     */
    public static void closeStatement(Statement state) {
        if (state != null) {
            try {
                state.close();
            } catch (Exception e) {
                logger.error("关闭jdbc Statement 失败！", e);
            }
        }
    }

    /**
     * 关闭 ResultSet
     *
     * @param rs
     */
    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error("关闭jdbc ResultSet 失败！", e);
            }
        }
    }

    /**
     * 提交
     *
     * @param conn
     */
    public static void commit(Connection conn) {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                logger.error("提交事务失败!", e);
            }
        }
    }

    /**
     * 回滚
     *
     * @param conn
     */
    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                logger.error("事务回滚失败!", e);
            }
        }
    }

    public static void close(ResultSet rs) {
        closeResultSet(rs);
    }

    public static void close(Statement state) {
        closeStatement(state);
    }

    public static void close(Connection conn) {
        closeConnection(conn);
    }

    // ======================== 以下为从 JdbcUtils 合并的通用查询/元数据方法 ========================

    private static final int SAMPLE_DATA_LIMIT = 1;

    /**
     * 获取多张表的元数据（表结构 + 样例数据）。
     */
    public static List<TableMetaData> getTablesMetadata(Connection connection, List<String> tableNames) throws SQLException {
        if (tableNames == null || tableNames.isEmpty()) {
            return Collections.emptyList();
        }
        List<TableMetaData> result = new ArrayList<>();
        for (String tableName : tableNames) {
            result.add(getTableMetadata(connection, tableName));
        }
        return result;
    }

    /**
     * 获取单表元数据。
     */
    public static TableMetaData getTableMetadata(Connection connection, String tableName) throws SQLException {
        logger.debug("获取表元数据: {}", tableName);

        TableMetaData.TableMetaDataBuilder builder = TableMetaData.builder().tableName(tableName);
        List<ColumnMetaData> columns = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();

        builder.tableComment(getTableComment(metaData, tableName));
        columns.addAll(getColumnMetadata(metaData, tableName));
        setPrimaryKeys(metaData, tableName, columns);

        Map<String, Object> sampleData = getSampleData(connection, tableName);
        builder.sampleData(sampleData);

        if (sampleData != null) {
            for (ColumnMetaData column : columns) {
                if (sampleData.containsKey(column.getColumnName())) {
                    Object value = sampleData.get(column.getColumnName());
                    column.setSampleValue(value != null ? safeToString(value) : "NULL");
                }
            }
        }

        builder.columns(columns);
        return builder.build();
    }

    private static String getTableComment(DatabaseMetaData metaData, String tableName) throws SQLException {
        try (ResultSet rs = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
            if (rs.next()) {
                return rs.getString("REMARKS");
            }
        }
        return "";
    }

    private static List<ColumnMetaData> getColumnMetadata(DatabaseMetaData metaData, String tableName) throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();

        try (ResultSet rs = metaData.getColumns(null, null, tableName, "%")) {
            while (rs.next()) {
                ColumnMetaData column = ColumnMetaData.builder()
                        .columnName(rs.getString("COLUMN_NAME"))
                        .columnType(rs.getString("TYPE_NAME"))
                        .columnSize(rs.getInt("COLUMN_SIZE"))
                        .decimalDigits(rs.getInt("DECIMAL_DIGITS"))
                        .isNullable(rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable)
                        .columnComment(rs.getString("REMARKS"))
                        .build();
                columns.add(column);
            }
        }

        return columns;
    }

    private static void setPrimaryKeys(DatabaseMetaData metaData, String tableName, List<ColumnMetaData> columns) throws SQLException {
        Set<String> pkColumns = new HashSet<>();

        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
            while (rs.next()) {
                pkColumns.add(rs.getString("COLUMN_NAME"));
            }
        }

        for (ColumnMetaData col : columns) {
            if (pkColumns.contains(col.getColumnName())) {
                col.setPrimaryKey(true);
            }
        }
    }

    private static String safeToString(Object obj) {
        if (obj == null) {
            return "NULL";
        }
        return obj.toString();
    }

    /**
     * 获取单行样例数据。
     */
    public static Map<String, Object> getSampleData(Connection conn, String tableName) {
        Map<String, Object> sampleData = new HashMap<>();

        String query = "SELECT * FROM " + tableName + " LIMIT " + SAMPLE_DATA_LIMIT;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            if (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    sampleData.put(columnName, value);
                }
            }
        } catch (SQLException e) {
            logger.warn("获取样例数据失败，表：{}", tableName, e);
        }

        return sampleData;
    }

    /**
     * 通用查询执行，返回 QueryResult。
     */
    public static QueryResult executeQuery(Connection conn, String sql) {
        long start = System.currentTimeMillis();
        List<Map<String, Object>> data = new ArrayList<>();
        int rowCount = 0;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    if (columnName == null || columnName.isEmpty()) {
                        columnName = metaData.getColumnName(i);
                    }
                    Object value = rs.getObject(i);
                    row.put(columnName, convertValue(value));
                }
                data.add(row);
                rowCount++;
            }

            long cost = System.currentTimeMillis() - start;
            return QueryResult.builder()
                    .sql(sql)
                    .data(data)
                    .rowCount(rowCount)
                    .executionTime(cost)
                    .success(true)
                    .build();
        } catch (SQLException e) {
            logger.error("执行SQL失败: {}", sql, e);
            long cost = System.currentTimeMillis() - start;
            return QueryResult.builder()
                    .sql(sql)
                    .data(Collections.emptyList())
                    .rowCount(0)
                    .executionTime(cost)
                    .success(false)
                    .errorMessage(e.getMessage())
                    .build();
        }
    }

    /**
     * 将数据库值安全转换为更适合 JSON 的类型。
     */
    private static Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate().toString();
        }
        if (value instanceof java.sql.Time) {
            return ((java.sql.Time) value).toLocalTime().toString();
        }
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toInstant().toString();
        }
        if (value instanceof LocalDate || value instanceof LocalTime || value instanceof LocalDateTime) {
            return value.toString();
        }
        if (value instanceof Clob) {
            try {
                Clob clob = (Clob) value;
                return clob.getSubString(1, (int) clob.length());
            } catch (SQLException e) {
                logger.warn("读取CLOB失败", e);
                return null;
            }
        }
        if (value instanceof Blob) {
            try {
                Blob blob = (Blob) value;
                int len = (int) blob.length();
                byte[] bytes = blob.getBytes(1, len);
                return "BLOB[" + bytes.length + "]";
            } catch (SQLException e) {
                logger.warn("读取BLOB失败", e);
                return null;
            }
        }
        return value;
    }

}
