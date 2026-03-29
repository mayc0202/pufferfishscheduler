package com.pufferfishscheduler.plugin.common.util;


import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;

/**
 * JDBC URL 与 Driver 构造工具。
 */
public class JdbcUrlUtil {

    // JDBC FORMAT
    private static final String MYSQL_URL_FORMAT = "jdbc:mysql://%s:%s/%s";
    private static final String ORACLE_SERVICE_URL_FORMAT = "jdbc:oracle:thin:@//%s:%s/%s";
    private static final String ORACLE_SID_URL_FORMAT = "jdbc:oracle:thin:@%s:%s:%s";
    private static final String POSTGRESQL_URL_FORMAT = "jdbc:postgresql://%s:%s/%s";
    private static final String SQLSERVER_URL_FORMAT = "jdbc:sqlserver://%s:%s;DatabaseName=%s";
    private static final String DM_URL_FORMAT = "jdbc:dm://%s:%s/%s";

    // DRIVER
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_DRIVER_8 = "com.mysql.cj.jdbc.Driver";
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";
    private static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String DM_DRIVER = "dm.jdbc.driver.DmDriver";

    /**
     * 构建 JDBC URL。
     *
     * @param type      数据库类型（Constants.DATABASE_TYPE）
     * @param host      主机
     * @param port      端口
     * @param dbName    数据库名
     * @param extConfig 扩展配置（JSON），仅部分类型使用（如 Oracle）
     */
    public static String getUrl(String type, String host, String port, String dbName, String extConfig) {
        if (type == null) {
            return null;
        }

        if (Constants.DATABASE_TYPE.MYSQL.equals(type)
                || Constants.DATABASE_TYPE.DORIS.equals(type)) {
            return String.format(MYSQL_URL_FORMAT, host, port, dbName);
        }

        if (Constants.DATABASE_TYPE.ORACLE.equals(type)) {
            String connectType = null;
            if (extConfig != null && !extConfig.isEmpty()) {
                JSONObject jsonObject = JSONObject.parseObject(extConfig);
                connectType = jsonObject.getString(Constants.DATABASE_EXT_CONFIG.ORACLE_CONNECT_TYPE);
            }
            if (Constants.ORACLE_CONNECT_TYPE.SID.equals(connectType)) {
                return String.format(ORACLE_SID_URL_FORMAT, host, port, dbName);
            }
            return String.format(ORACLE_SERVICE_URL_FORMAT, host, port, dbName);
        }

        if (Constants.DATABASE_TYPE.POSTGRESQL.equals(type)) {
            return String.format(POSTGRESQL_URL_FORMAT, host, port, dbName);
        }
        if (Constants.DATABASE_TYPE.SQL_SERVER.equals(type)) {
            return String.format(SQLSERVER_URL_FORMAT, host, port, dbName);
        }
        if (Constants.DATABASE_TYPE.DM8.equals(type)) {
            return String.format(DM_URL_FORMAT, host, port, dbName);
        }

        // 未匹配到已知类型时返回 null，调用方按需处理
        return null;
    }

    /**
     * 获取 JDBC Driver 类名。
     */
    public static String getDriver(String type) {
        if (type == null) {
            return null;
        }

        if (Constants.DATABASE_TYPE.MYSQL.equals(type)
                || Constants.DATABASE_TYPE.DORIS.equals(type)) {
            return MYSQL_DRIVER;
        }
        if (Constants.DATABASE_TYPE.ORACLE.equals(type)) {
            return ORACLE_DRIVER;
        }
        if (Constants.DATABASE_TYPE.POSTGRESQL.equals(type)) {
            return POSTGRESQL_DRIVER;
        }
        if (Constants.DATABASE_TYPE.SQL_SERVER.equals(type)) {
            return SQLSERVER_DRIVER;
        }
        if (Constants.DATABASE_TYPE.DM8.equals(type)) {
            return DM_DRIVER;
        }

        return null;
    }

}
