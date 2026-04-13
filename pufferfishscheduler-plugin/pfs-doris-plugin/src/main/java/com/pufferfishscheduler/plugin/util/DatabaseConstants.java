package com.pufferfishscheduler.plugin.util;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author: yc
 * @CreateTime: 2024-12-20
 * @Description:
 * @Version: 1.0
 */
public class DatabaseConstants {

    public final static String MYSQL = "MySQL";
    public final static String ORACLE = "Oracle";
    public final static String SQL_SERVER = "SQLServer";
    public final static String MS_SQL_SERVER = "MS SQL Server";
    public final static String POSTGRESQL = "PostgreSQL";

    public final static String GENERIC = "GENERIC";
    public final static String KYUUBI = "Kyuubi";
    public final static String DM8 = "DM8";
    public final static String CACHE = "Cache";
    public final static String TIDB = "TiDB";

    public final static String DORIS = "Doris";
    public final static String STAR_ROCKS = "StarRocks";

    public final static String SID = "sid";
    public final static String SERVICE = "service";

    /**
     * 增量同步方式
     */
    public final static String INCREMENT_TYPE_NUMBER_TYPE = "1";
    public final static String INCREMENT_TYPE_DATE_TYPE = "2";

    // sql条件符号
    public final static String AND = "and";
    public final static String OR = "or";
    public final static String less ="<";
    public final static String lessEqual ="<=";
    public final static String great =">";
    public final static String greatEqual =">=";
    public final static String equal ="=";
    public final static String notEqual ="!=";
    public final static String before = "before";
    public final static String after = "after";
    public final static String like = "like";
    public final static String MOD = "MOD";

    public final static String DATE = "date";
    public final static String DATETIME = "datetime";
    public final static String TIMESTAMP = "timestamp";

    // 存储时间日期类型
    public final static List<String> DATE_TIME_TYPE = new LinkedList<>();

    static {
        DATE_TIME_TYPE.add(DATE);
        DATE_TIME_TYPE.add(DATETIME);
        DATE_TIME_TYPE.add(TIMESTAMP);
    }

    /**
     * 监听数据库类型
     *
     * @param dbType
     * @return
     */
    public static String transDatabseType(String dbType) {
        switch (dbType) {
            case TIDB:
            case STAR_ROCKS:
            case DORIS:
            case MYSQL:
                return MYSQL;
            case CACHE:
            case KYUUBI:
            case DM8:
                return GENERIC;
            case SQL_SERVER:
                return MS_SQL_SERVER;
            default:
                return dbType;
        }
    }

}