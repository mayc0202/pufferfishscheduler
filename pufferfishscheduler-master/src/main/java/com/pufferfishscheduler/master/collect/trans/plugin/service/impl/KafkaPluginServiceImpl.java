package com.pufferfishscheduler.master.collect.trans.plugin.service.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.enums.DatabaseCategory;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.master.collect.trans.plugin.service.KafkaPluginService;
import com.pufferfishscheduler.master.database.connect.mq.AbstractMQConnector;
import com.pufferfishscheduler.master.database.connect.mq.MQConnectorFactory;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.master.database.database.service.DbGroupService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Slf4j
@Service
public class KafkaPluginServiceImpl implements KafkaPluginService {

    @Autowired
    private DbGroupService dbGroupService;

    @Autowired
    private DbDatabaseService databaseService;

    /**
     * 获取MQ数据源树形结构
     *
     * @return
     */
    @Override
    public List<Tree> mqDbTree() {
        return dbGroupService.dbTreeByCategory(DatabaseCategory.MQ);
    }

    /**
     * 获取topic列表
     *
     * @param id
     * @return
     */
    @Override
    public List<String> topics(Integer id) {

        DbDatabase database = databaseService.getDatabaseById(id);
        if (!Constants.DATABASE_TYPE.KAFKA.equals(database.getType())) {
            throw new BusinessException(String.format("请校验[id=%s]的数据源是否是Kafka数据源！", id));
        }

        // 构造kafka连接器
        AbstractMQConnector connector = MQConnectorFactory.getConnector(database.getType());
        connector.setId(database.getId());
        connector.setDbHost(database.getDbHost());
        connector.setDbPort(database.getDbPort());
        connector.setType(database.getType());
        connector.setProperties(database.getProperties());

        return connector.topics();
    }
}
