package com.pufferfishscheduler.common.utils;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.domain.ColumnMetadata;
import com.pufferfishscheduler.domain.domain.DatabaseMetadata;
import com.pufferfishscheduler.domain.domain.QueryResult;
import com.pufferfishscheduler.domain.domain.TableMetadata;
import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * JDBC工具类
 *
 * @author Mayc
 * @since 2026-02-24 16:30
 */
@Slf4j
public class JdbcUtils {

    private static final int DEFAULT_TIMEOUT = 10;
    private static final int SAMPLE_DATA_LIMIT = 1;
    private static final String MYSQL_PROPERTIES = "useCursorFetch=true&useSSL=false&useUnicode=yes&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&serverTimezone=Asia/Shanghai&useOldAliasMetadataBehavior=true";

    private JdbcUtils() {
        // 私有构造函数，防止实例化
    }

    /**
     * 获取数据库连接
     */
    public static Connection getConnection(String driverName, String url, DatabaseMetadata metadata) {
        Connection conn = null;
        try {
            Class.forName(driverName);
            DriverManager.setLoginTimeout(DEFAULT_TIMEOUT);

            Properties properties = buildConnectionProperties(metadata);
            conn = DriverManager.getConnection(url, properties);

            log.info("数据库连接成功 - 类型: {}, 数据库: {}, 用户名: {}",
                    metadata.getType(), metadata.getDbName(), metadata.getUsername());
            return conn;

        } catch (ClassNotFoundException e) {
            log.error("数据库驱动加载失败: {}", driverName, e);
            throw new BusinessException("数据库驱动加载失败: " + driverName);
        } catch (SQLException e) {
            log.error("获取数据库连接失败 - URL: {}, 用户名: {}", url, metadata.getUsername(), e);
            throw new BusinessException("数据库连接失败: " + e.getMessage());
        } catch (Exception e) {
            log.error("获取数据库连接发生未知错误", e);
            throw new BusinessException("数据库连接异常: " + e.getMessage());
        }
    }

    /**
     * 构建连接属性
     */
    private static Properties buildConnectionProperties(DatabaseMetadata metadata) {
        Properties properties = new Properties();

        properties.put("user", metadata.getUsername());
        properties.put("password", metadata.getPassword());

        setSchema(properties, metadata);

        if (Constants.DbType.mysql.equals(metadata.getType())) {
            setMySqlProperties(properties);
        }

        if (StringUtils.isNotBlank(metadata.getProperties())) {
            try {
                HashMap<String, String> customProps = JSONObject.parseObject(metadata.getProperties(), HashMap.class);
                properties.putAll(customProps);
            } catch (Exception e) {
                log.warn("解析自定义属性失败: {}", metadata.getProperties(), e);
            }
        }

        return properties;
    }

    /**
     * 设置Schema
     */
    private static void setSchema(Properties properties, DatabaseMetadata metadata) {
        if (StringUtils.isBlank(metadata.getDbSchema())) {
            return;
        }

        String dbType = metadata.getType();
        String schemaKey = getSchemaKey(dbType);
        if (schemaKey != null) {
            properties.setProperty(schemaKey, metadata.getDbSchema());
        }
    }

    /**
     * 获取Schema对应的Key
     */
    private static String getSchemaKey(String dbType) {
        if (Constants.DbType.postgresql.equals(dbType) ||
                Constants.DbType.vastbaseg100.equals(dbType) ||
                Constants.DbType.gaussdb.equals(dbType) ||
                Constants.DbType.kingbase.equals(dbType)) {
            return "currentSchema";
        } else if (Constants.DbType.oracle.equals(dbType)) {
            return "schema";
        }
        return null;
    }

    /**
     * 设置MySQL特定属性
     */
    private static void setMySqlProperties(Properties properties) {
        String[] props = MYSQL_PROPERTIES.split("&");
        for (String prop : props) {
            String[] kv = prop.split("=");
            if (kv.length == 2) {
                properties.put(kv[0], kv[1]);
            }
        }
    }

    /**
     * 关闭连接
     */
    public static void closeQuietly(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    log.warn("关闭资源失败: {}", closeable.getClass().getSimpleName(), e);
                }
            }
        }
    }

    /**
     * 获取表的元数据
     */
    public static TableMetadata getTableMetadata(Connection connection, String tableName) throws SQLException {
        log.debug("获取表元数据: {}", tableName);

        TableMetadata.TableMetadataBuilder builder = TableMetadata.builder().tableName(tableName);
        List<ColumnMetadata> columns = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();

        builder.tableComment(getTableComment(metaData, tableName));
        columns.addAll(getColumnMetadata(metaData, tableName));
        setPrimaryKeys(metaData, tableName, columns);

        // 获取样例数据 - 使用安全的方法
        Map<String, Object> sampleData = getSampleData(connection, tableName);
        builder.sampleData(sampleData);

        // 为列设置样例值
        for (ColumnMetadata column : columns) {
            if (sampleData != null && sampleData.containsKey(column.getColumnName())) {
                Object value = sampleData.get(column.getColumnName());
                column.setSampleValue(value != null ? safeToString(value) : "NULL");
            }
        }

        builder.columns(columns);

        return builder.build();
    }

    /**
     * 获取表注释
     */
    private static String getTableComment(DatabaseMetaData metaData, String tableName) throws SQLException {
        try (ResultSet rs = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
            if (rs.next()) {
                return rs.getString("REMARKS");
            }
        }
        return "";
    }

    /**
     * 获取列元数据
     */
    private static List<ColumnMetadata> getColumnMetadata(DatabaseMetaData metaData, String tableName) throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();

        try (ResultSet rs = metaData.getColumns(null, null, tableName, "%")) {
            while (rs.next()) {
                ColumnMetadata column = ColumnMetadata.builder()
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

    /**
     * 设置主键信息
     */
    private static void setPrimaryKeys(DatabaseMetaData metaData, String tableName, List<ColumnMetadata> columns) throws SQLException {
        Set<String> pkColumns = new HashSet<>();

        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
            while (rs.next()) {
                pkColumns.add(rs.getString("COLUMN_NAME"));
            }
        }

        columns.stream()
                .filter(col -> pkColumns.contains(col.getColumnName()))
                .forEach(col -> col.setPrimaryKey(true));
    }

    /**
     * 安全地将对象转换为字符串
     */
    private static String safeToString(Object obj) {
        if (obj == null) return "NULL";
        return obj.toString();
    }

    /**
     * 获取样例数据 - 通用版本，不依赖特定数据库类型
     */
    public static Map<String, Object> getSampleData(Connection conn, String tableName) {
        Map<String, Object> sampleData = new HashMap<>();

        String query = "SELECT * FROM " + tableName + " LIMIT 1";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            if (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);

                    // 使用通用的 toString 方法，避免特定类型转换
                    if (value != null) {
                        // 对于时间类型，尝试获取字符串表示
                        if (value instanceof java.util.Date ||
                                value instanceof LocalDate ||
                                value instanceof LocalDateTime ||
                                value instanceof LocalTime) {
                            try {
                                // 尝试使用 ResultSet 的 getString 方法获取字符串表示
                                value = rs.getString(i);
                            } catch (SQLException e) {
                                // 如果失败，使用 toString
                                value = value.toString();
                            }
                        } else {
                            value = value.toString();
                        }
                    }

                    sampleData.put(columnName, value);
                }
            }

            log.debug("成功获取表 {} 的样例数据，共 {} 列", tableName, sampleData.size());

        } catch (SQLException e) {
            log.warn("获取表 {} 的样例数据失败: {}", tableName, e.getMessage());
            // 返回空Map，不中断流程
        }

        return sampleData;
    }

    /**
     * 获取多个表的元数据
     */
    public static List<TableMetadata> getTablesMetadata(Connection connection, List<String> tableNames) {
        return tableNames.stream()
                .map(tableName -> {
                    try {
                        return getTableMetadata(connection, tableName);
                    } catch (SQLException e) {
                        log.warn("获取表元数据失败: {}", tableName, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 执行SQL查询
     */
    public static QueryResult executeQuery(Connection connection, String sql) {
        long startTime = System.currentTimeMillis();
        QueryResult.QueryResultBuilder builder = QueryResult.builder()
                .sql(sql)
                .success(false);

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            List<Map<String, Object>> results = extractResultSet(rs);

            builder.data(results)
                    .rowCount(results.size())
                    .success(true);

        } catch (SQLException e) {
            log.error("执行SQL失败: {}", sql, e);
            builder.errorMessage(e.getMessage());
        }

        builder.executionTime(System.currentTimeMillis() - startTime);
        return builder.build();
    }

    /**
     * 提取ResultSet结果
     */
    private static List<Map<String, Object>> extractResultSet(ResultSet rs) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                Object value = rs.getObject(i);

                // 统一转换为字符串，避免类型问题
                if (value != null) {
                    value = value.toString();
                }

                row.put(columnName, value);
            }
            results.add(row);
        }

        return results;
    }
}