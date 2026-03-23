package com.pufferfishscheduler.worker.task.metadata.connect;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.worker.task.metadata.connect.impl.DorisConnector;
import com.pufferfishscheduler.worker.task.metadata.connect.impl.MySQLConnector;

public class DatabaseConnectorFactory {

    public static AbstractDatabaseConnector getConnector(String type) {
        switch (type) {
            case Constants.DATABASE_TYPE.MYSQL:
                return new MySQLConnector();
            case Constants.DATABASE_TYPE.DORIS:
                return new DorisConnector();
            default:
                throw new BusinessException(String.format("Worker 暂不支持此类型数据库元数据采集: %s", type));
        }
    }
}

