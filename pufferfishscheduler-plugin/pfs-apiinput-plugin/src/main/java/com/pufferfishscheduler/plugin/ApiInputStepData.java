package com.pufferfishscheduler.plugin;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class ApiInputStepData extends BaseStepData implements StepDataInterface {
    public RowMetaInterface outputRowMeta;
    public RowMetaInterface inputRowMeta;
    /** 无上游步骤时，首轮用空行元数据拉取 API 后置位，避免再次进入 processRow 重复请求 */
    public boolean standaloneRunCompleted;
}
