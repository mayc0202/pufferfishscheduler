package com.pufferfishscheduler.common.utils;

import com.pufferfishscheduler.common.constants.Constants;

/**
 * 监听数据库类型
 */
public class ListenDatabaseType {
    /**
     * 监听数据库类型
     * 
     * @param dbType
     * @return
     */
    public static String listenDatabseType(String dbType) {
        switch (dbType) {
            case Constants.DbType.mysql:
            case Constants.DbType.starRocks:
            case Constants.DbType.doris:
                return Constants.Kettle_DataBaseType.mysql;
            case Constants.DbType.dm:
                return Constants.Kettle_DataBaseType.generic;
            case Constants.DbType.sqlServer:
                return Constants.Kettle_DataBaseType.sqlServer;
            default:
                return dbType;
        }
    }
}
