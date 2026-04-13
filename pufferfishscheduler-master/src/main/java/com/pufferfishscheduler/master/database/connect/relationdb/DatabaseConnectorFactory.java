package com.pufferfishscheduler.master.database.connect.relationdb;


import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.database.connect.relationdb.impl.*;

/**
 * 数据源连接器工厂
 */
public class DatabaseConnectorFactory {

    /**
     *  获取数据源连接器
     *
     * @param type 数据源类型
     * @return
     */
    public static AbstractDatabaseConnector getConnector(String type) {
        return switch (type) {
            case Constants.DATABASE_TYPE.MYSQL -> new MySQLConnector();
            case Constants.DATABASE_TYPE.ORACLE -> new OracleConnector();
            case Constants.DATABASE_TYPE.POSTGRESQL -> new PostgreSQLConnector();
            case Constants.DATABASE_TYPE.SQL_SERVER -> new SQLServerConnector();
            case Constants.DATABASE_TYPE.DM8 -> new Dm8Connector();
            case Constants.DATABASE_TYPE.DORIS -> new DorisConnector();
            case Constants.DATABASE_TYPE.STAR_ROCKS -> new StarRocksConnector();
            default -> throw new BusinessException(String.format("暂不支持[%s]此类型数据库!", type));
        };
    }

}
