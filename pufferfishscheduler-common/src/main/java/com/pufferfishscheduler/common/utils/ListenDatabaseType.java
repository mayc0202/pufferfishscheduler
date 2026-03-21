package com.pufferfishscheduler.common.utils;

import com.pufferfishscheduler.common.constants.Constants;

/**
 * 监听数据库类型
 */
public class ListenDatabaseType {
    /**
     * 监听数据库类型
     *
     * @param dbType 数据库类型
     * @return
     */
    public static String listenDatabaseType(String dbType) {
        switch (dbType) {
            case Constants.DATABASE_TYPE.MYSQL:
            case Constants.DATABASE_TYPE.DORIS:
                return Constants.Kettle_DataBaseType.mysql;
            case Constants.DATABASE_TYPE.POSTGRESQL:
                return Constants.Kettle_DataBaseType.postgresql;
            case Constants.DATABASE_TYPE.ORACLE:
                return Constants.Kettle_DataBaseType.oracle;
            case Constants.DATABASE_TYPE.DM8:
                return Constants.Kettle_DataBaseType.generic;
            case Constants.DATABASE_TYPE.SQL_SERVER:
                return Constants.Kettle_DataBaseType.sqlServer;
            default:
                return dbType;
        }
    }
}
