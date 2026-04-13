package com.pufferfishscheduler.master.database.connect.nosql;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.database.connect.nosql.impl.RedisConnector;

/**
 * NoSQL数据库连接工厂
 */
public class NoSqlConnectorFactory {
    /**
     * 获取NoSQL数据库连接器
     *
     * @param type 数据库类型
     * @return NoSQL数据库连接器
     */
    public static AbstractNoSqlConnector getConnector(String type) {
        return switch (type) {
            case Constants.DATABASE_TYPE.REDIS -> new RedisConnector();
            default -> throw new BusinessException(String.format("暂不支持[%s]此类型非关系型数据库!", type));
        };
    }
}