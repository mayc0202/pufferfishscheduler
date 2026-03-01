package com.pufferfishscheduler.common.utils;


import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;

public class JdbcUrlUtil {

    // JDBC FORMAT
    private static final String MYSQL_URL_FORMAT = "jdbc:mysql://%s:%s/%s";

    private static final String ORACLE_SERVICE_URL_FORMAT = "jdbc:oracle:thin:@//%s:%s/%s";

    private static final String ORACLE_SID_URL_FORMAT = "jdbc:oracle:thin:@%s:%s:%s";

    private static final String POSTGRESQL_URL_FORMAT = "jdbc:postgresql://%s:%s/%s";

    private static final String SQLSERVER_URL_FORMAT = "jdbc:sqlserver://%s:%s;DatabaseName=%s";

    private static final String CACHE_URL_FORMAT = "jdbc:IRIS://%s:%s/%s";

    private static final String DM_URL_FORMAT = "jdbc:dm://%s:%s/%s";

    private static final String GAUSSDB_URL_FORMAT = "jdbc:opengauss://%s:%s/%s";

    private static final String KYUUBI_URL_FORMAT = "jdbc:kyuubi://%s:%s?kyuubi.engine.type=JDBC";

    private static final String VASTBASE_URL_FORMAT = "jdbc:vastbase://%s:%s/%s";

    private static final String KINGBASE_URL_FORMAT = "jdbc:kingbase8://%s:%s/%s";

    // DRIVER
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

    private static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static final String CACHE_DRIVER = "com.intersystems.jdbc.IRISDriver";

    private static final String DM_DRIVER = "dm.jdbc.driver.DmDriver";

    private static final String GAUSSDB_DRIVER = "com.huawei.opengauss.jdbc.Driver";

    private static final String KYUUBI_DRIVER = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";

    private static final String VASTBASE_DRIVER = "cn.com.vastbase.Driver";

    private static final String KINGBASE_DRIVER = "com.kingbase8.Driver";

    public static String getUrl(String type, String host, String port, String dbName,String extConfig) {
        String jdbcUrl = null;

        if (Constants.DbType.mysql.equals(type) ||
                Constants.DbType.doris.equals(type) ||
                Constants.DbType.starRocks.equals(type) ||
                Constants.DbType.tidb.equals(type)) {
            jdbcUrl = String.format(MYSQL_URL_FORMAT, host, port, dbName);
        } else if (Constants.DbType.oracle.equals(type)) {
            JSONObject jsonObject = JSONObject.parseObject(extConfig);
            String connectType = jsonObject.getString(Constants.DATABASE_EXT_CONFIG.ORACLE_CONNECT_TYPE);
            if (connectType!=null && connectType.equals(Constants.ORACLE_CONNECT_TYPE.SID)) {
                jdbcUrl = String.format(ORACLE_SID_URL_FORMAT, host, port, dbName);
            }else {
                jdbcUrl = String.format(ORACLE_SERVICE_URL_FORMAT, host, port, dbName);
            }
        } else if (Constants.DbType.postgresql.equals(type)) {
            jdbcUrl = String.format(POSTGRESQL_URL_FORMAT, host, port, dbName);
        } else if (Constants.DbType.sqlServer.equals(type)) {
            jdbcUrl = String.format(SQLSERVER_URL_FORMAT, host, port, dbName);
        } else if (Constants.DbType.cache.equals(type)) {
            jdbcUrl = String.format(CACHE_URL_FORMAT, host, port, dbName);
        } else if (Constants.DbType.kyuubi.equals(type)) {
            jdbcUrl = String.format(KYUUBI_URL_FORMAT, host, port);
        } else if (Constants.DbType.dm.equals(type)) {
            jdbcUrl = String.format(DM_URL_FORMAT, host, port, dbName);
        } else if (Constants.DbType.vastbaseg100.equals(type)) {
            jdbcUrl = String.format(VASTBASE_URL_FORMAT, host, port, dbName);
        } else if (Constants.DbType.gaussdb.equals(type)) {
            jdbcUrl = String.format(GAUSSDB_URL_FORMAT, host, port, dbName);
        } else if (Constants.DbType.kingbase.equals(type)) {
            jdbcUrl = String.format(KINGBASE_URL_FORMAT, host, port, dbName);
        }
        return jdbcUrl;
    }

    public static String getDriver(String type) {
        String driverName = null;

        if (Constants.DbType.mysql.equals(type) ||
                Constants.DbType.doris.equals(type) ||
                Constants.DbType.starRocks.equals(type) ||
                Constants.DbType.tidb.equals(type)) {
            driverName = MYSQL_DRIVER;
        } else if (Constants.DbType.oracle.equals(type)) {
            driverName = ORACLE_DRIVER;
        } else if (Constants.DbType.postgresql.equals(type)) {
            driverName = POSTGRESQL_DRIVER;
        } else if (Constants.DbType.sqlServer.equals(type)) {
            driverName = SQLSERVER_DRIVER;
        } else if (Constants.DbType.cache.equals(type)) {
            driverName = CACHE_DRIVER;
        } else if (Constants.DbType.kyuubi.equals(type)) {
            driverName = KYUUBI_DRIVER;
        } else if (Constants.DbType.dm.equals(type)) {
            driverName = DM_DRIVER;
        } else if (Constants.DbType.vastbaseg100.equals(type)) {
            driverName = VASTBASE_DRIVER;
        } else if (Constants.DbType.gaussdb.equals(type)) {
            driverName = GAUSSDB_DRIVER;
        } else if(Constants.DbType.kingbase.equals(type)){
            driverName = KINGBASE_DRIVER;
        }

        return driverName;
    }

}
