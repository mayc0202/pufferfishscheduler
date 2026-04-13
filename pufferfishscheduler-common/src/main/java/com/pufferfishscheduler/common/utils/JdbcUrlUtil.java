package com.pufferfishscheduler.common.utils;


import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;

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

        switch (type) {
            case Constants.DATABASE_TYPE.MYSQL,
                 Constants.DATABASE_TYPE.DORIS,
                 Constants.DATABASE_TYPE.STAR_ROCKS -> {
                return String.format(MYSQL_URL_FORMAT, host, port, dbName);
            }
            case Constants.DATABASE_TYPE.ORACLE -> {
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
            case Constants.DATABASE_TYPE.POSTGRESQL -> {
                return String.format(POSTGRESQL_URL_FORMAT, host, port, dbName);
            }
            case Constants.DATABASE_TYPE.SQL_SERVER -> {
                return String.format(SQLSERVER_URL_FORMAT, host, port, dbName);
            }
            case Constants.DATABASE_TYPE.DM8 -> {
                return String.format(DM_URL_FORMAT, host, port, dbName);
            }
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

        return switch (type) {
            case Constants.DATABASE_TYPE.MYSQL,
                 Constants.DATABASE_TYPE.DORIS
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

}
