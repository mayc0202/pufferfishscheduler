package com.pufferfishscheduler.trans.engine;

import java.util.List;

import com.pufferfishscheduler.common.utils.CommonUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import com.pufferfishscheduler.trans.engine.entity.TransParam;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * 数据采集转换
 * 
 * @author Mayc
 * @since 2025-09-07 01:02
 */
@Slf4j
@Data
public class TransWrapper extends Trans {
    private String instanceId;
    private String taskFlowLogId;
    private String executingType; // manual-手工,timing-定时
    private String businessType; // 业务类型
    private String businessNo; // 业务表ID
    private String parentInstanceId;
    private Long dataVolume; // 数据量。取流程执行的输出数量
    private List<TransParam> params;
    private boolean executeStatus; // 执行状态

    public TransWrapper() {
        this.instanceId = CommonUtil.getUUIDString();
    }

    public TransWrapper(TransMeta transMeta) {
        super(transMeta);
        this.instanceId = CommonUtil.getUUIDString();
    }
}
