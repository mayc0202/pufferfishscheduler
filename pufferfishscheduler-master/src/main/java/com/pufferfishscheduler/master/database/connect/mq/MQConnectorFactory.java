package com.pufferfishscheduler.master.database.connect.mq;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.database.connect.mq.impl.KafkaConnector;
import com.pufferfishscheduler.master.database.connect.mq.impl.RabbitMQConnector;

/**
 * 消息队列连接器工厂
 */
public class MQConnectorFactory {

    /**
     *  获取数据源连接器
     *
     * @param type 数据源类型
     * @return
     */
    public static AbstractMQConnector getConnector(String type) {
        return switch (type) {
            case Constants.DATABASE_TYPE.KAFKA -> new KafkaConnector();
            case Constants.DATABASE_TYPE.RABBITMQ -> new RabbitMQConnector();
            default -> throw new BusinessException(String.format("暂不支持[%s]此类型MQ!", type));
        };
    }
}
