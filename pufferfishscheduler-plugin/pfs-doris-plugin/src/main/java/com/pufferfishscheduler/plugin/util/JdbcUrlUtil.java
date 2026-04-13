package com.pufferfishscheduler.plugin.util;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * JDBC URL 与 驱动类 工具类
 * 纯Java实现，无Lombok
 */
public class JdbcUrlUtil {

    // ========================= URL 格式常量 =========================
    private static final String MYSQL_URL_FORMAT = "jdbc:mysql://%s:%s/%s";
    private static final String ORACLE_SERVICE_URL_FORMAT = "jdbc:oracle:thin:@//%s:%s/%s";
    private static final String ORACLE_SID_URL_FORMAT = "jdbc:oracle:thin:@%s:%s:%s";
    private static final String POSTGRESQL_URL_FORMAT = "jdbc:postgresql://%s:%s/%s";
    private static final String SQLSERVER_URL_FORMAT = "jdbc:sqlserver://%s:%s;DatabaseName=%s";
    private static final String DM_URL_FORMAT = "jdbc:dm://%s:%s/%s";

    // ========================= 驱动类常量 =========================
    private static final String DRIVER_MYSQL = "com.mysql.jdbc.Driver";
    private static final String DRIVER_ORACLE = "oracle.jdbc.driver.OracleDriver";
    private static final String DRIVER_POSTGRESQL = "org.postgresql.Driver";
    private static final String DRIVER_SQLSERVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String DRIVER_CACHE = "com.intersystems.jdbc.IRISDriver";
    private static final String DRIVER_DM = "dm.jdbc.driver.DmDriver";

    // ========================= 驱动映射 =========================
    private static final Map<String, String> DRIVER_MAP = new HashMap<>();

    // ========================= URL 生成器映射 =========================
    private static final Map<String, UrlGenerator> URL_GENERATOR_MAP = new HashMap<>();

    /**
     * 私有构造，防止实例化
     * 替代 lombok @NoArgsConstructor(access = AccessLevel.PRIVATE)
     */
    private JdbcUrlUtil() {
        throw new AssertionError("工具类不可实例化");
    }

    static {
        // 初始化驱动
        DRIVER_MAP.put(Constants.DataBaseType.mysql, DRIVER_MYSQL);
        DRIVER_MAP.put(Constants.DataBaseType.doris, DRIVER_MYSQL);
        DRIVER_MAP.put(Constants.DataBaseType.oracle, DRIVER_ORACLE);
        DRIVER_MAP.put(Constants.DataBaseType.postgresql, DRIVER_POSTGRESQL);
        DRIVER_MAP.put(Constants.DataBaseType.sqlServer, DRIVER_SQLSERVER);
        DRIVER_MAP.put(Constants.DbType.sqlServer, DRIVER_SQLSERVER);
        DRIVER_MAP.put(Constants.DataBaseType.cache, DRIVER_CACHE);
        DRIVER_MAP.put(Constants.DataBaseType.dm, DRIVER_DM);

        // 初始化URL生成器
        URL_GENERATOR_MAP.put(Constants.DataBaseType.mysql, JdbcUrlUtil::buildMysqlUrl);
        URL_GENERATOR_MAP.put(Constants.DataBaseType.doris, JdbcUrlUtil::buildMysqlUrl);
        URL_GENERATOR_MAP.put(Constants.DataBaseType.oracle, JdbcUrlUtil::buildOracleUrl);
        URL_GENERATOR_MAP.put(Constants.DataBaseType.postgresql, JdbcUrlUtil::buildPostgreSqlUrl);
        URL_GENERATOR_MAP.put(Constants.DataBaseType.sqlServer, JdbcUrlUtil::buildSqlServerUrl);
        URL_GENERATOR_MAP.put(Constants.DbType.sqlServer, JdbcUrlUtil::buildSqlServerUrl);
        URL_GENERATOR_MAP.put(Constants.DataBaseType.dm, JdbcUrlUtil::buildDmUrl);
    }

    /**
     * 获取 JDBC URL
     */
    public static String getUrl(String type, String host, String port, String dbName, String extConfig) {
        if (StringUtils.isBlank(type) || StringUtils.isBlank(host)
                || StringUtils.isBlank(port) || StringUtils.isBlank(dbName)) {
            return null;
        }

        UrlGenerator generator = URL_GENERATOR_MAP.get(type);
        if (Objects.isNull(generator)) {
            return null;
        }

        return generator.generate(host, port, dbName, extConfig);
    }

    /**
     * 获取驱动类
     */
    public static String getDriver(String type) {
        if (StringUtils.isBlank(type)) {
            return null;
        }
        return DRIVER_MAP.get(type);
    }

    // ========================= URL 构建方法 =========================
    private static String buildMysqlUrl(String host, String port, String dbName, String extConfig) {
        return String.format(MYSQL_URL_FORMAT, host, port, dbName);
    }

    private static String buildOracleUrl(String host, String port, String dbName, String extConfig) {
        if (StringUtils.isBlank(extConfig)) {
            return String.format(ORACLE_SERVICE_URL_FORMAT, host, port, dbName);
        }

        try {
            JSONObject config = JSON.parseObject(extConfig);
            String connectType = config.getString(Constants.DATABASE_EXT_CONFIG.ORACLE_CONNECT_TYPE);
            if (Constants.ORACLE_CONNECT_TYPE.SID.equals(connectType)) {
                return String.format(ORACLE_SID_URL_FORMAT, host, port, dbName);
            }
        } catch (Exception ignored) {
            // 解析异常默认使用 SERVICE NAME
        }

        return String.format(ORACLE_SERVICE_URL_FORMAT, host, port, dbName);
    }

    private static String buildPostgreSqlUrl(String host, String port, String dbName, String extConfig) {
        return String.format(POSTGRESQL_URL_FORMAT, host, port, dbName);
    }

    private static String buildSqlServerUrl(String host, String port, String dbName, String extConfig) {
        return String.format(SQLSERVER_URL_FORMAT, host, port, dbName);
    }

    private static String buildDmUrl(String host, String port, String dbName, String extConfig) {
        return String.format(DM_URL_FORMAT, host, port, dbName);
    }

    /**
     * URL 生成函数式接口
     */
    @FunctionalInterface
    private interface UrlGenerator {
        String generate(String host, String port, String dbName, String extConfig);
    }
}