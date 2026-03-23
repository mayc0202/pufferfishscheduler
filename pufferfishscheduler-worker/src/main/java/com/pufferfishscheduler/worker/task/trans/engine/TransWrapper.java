package com.pufferfishscheduler.worker.task.trans.engine;

import com.pufferfishscheduler.common.utils.CommonUtil;
import com.pufferfishscheduler.worker.task.trans.engine.entity.TransParam;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import java.util.List;

/**
 * 数据采集转换
 * 
 * @author Mayc
 * @since 2025-09-07 01:02
 */
@Slf4j
@Data
public class TransWrapper extends Trans {
    /**
     * 转换流程定义 ID（多任务可共用同一流程，不得用作运行期缓存键）
     */
    private Integer flowId;
    /**
     * 调度侧转换任务 ID（可选，用于业务关联与幂等维度）
     */
    private Integer taskId;
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
