package com.pufferfishscheduler.worker.task.trans.engine.entity;

import com.pufferfishscheduler.common.enums.FlowType;
import com.pufferfishscheduler.dao.entity.KettleFlowRepository;

/**
 * 转换流程配置
 */
public class TransFlowConfig extends KettleFlowRepository {
    public TransFlowConfig() {
        super.setFlowType(FlowType.Trans.name());
    }

    /**
     * 构造函数
     * 
     * @param bizType 业务对象类型
     * @param bizObjectId 业务对象关联ID
     * @param flowContent   流程配置
     */
    public TransFlowConfig(String bizType, String bizObjectId, String flowContent) {
        this();
        this.setBizType(bizType);
        this.setBizObjectId(bizObjectId);
        this.setFlowContent(flowContent);
    }
}
