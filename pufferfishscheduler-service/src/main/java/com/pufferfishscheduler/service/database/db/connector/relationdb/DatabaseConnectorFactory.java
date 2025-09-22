package com.pufferfishscheduler.service.database.db.connector.relationdb;


import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.service.database.db.connector.relationdb.impl.DorisConnector;
import com.pufferfishscheduler.service.database.db.connector.relationdb.impl.MySQLConnector;

/**
 * 数据源连接器工厂
 */
public class DatabaseConnectorFactory {

    public static AbstractDatabaseConnector getConnector(String type) {
        switch (type) {
            case Constants.DATABASE_TYPE.MYSQL:
                return new MySQLConnector();
//            case Constants.DATABASE_TYPE.ORACLE:
//                return new OracleDBConnector();
//            case Constants.DATABASE_TYPE.POSTGRESQL:
//                return new PostgresqlConnector();
//            case Constants.DATABASE_TYPE.SQL_SERVER:
//                return new SqlServerDBConnector();
//            case Constants.DATABASE_TYPE.DM8:
//                return new DmDBConnector();
            case Constants.DATABASE_TYPE.DORIS:
                return new DorisConnector();
//            case Constants.DATABASE_TYPE.STAR_ROCKS:
//                return new StarRocksDBConnector();
            default:
                throw new BusinessException(String.format("暂不支持此类型数据库![type:%s]", type));
        }
    }

}
