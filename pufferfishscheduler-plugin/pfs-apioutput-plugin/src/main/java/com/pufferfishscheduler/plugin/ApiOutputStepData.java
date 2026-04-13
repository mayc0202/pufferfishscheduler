package com.pufferfishscheduler.plugin;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class ApiOutputStepData extends BaseStepData implements StepDataInterface {
    public RowMetaInterface outputRowMeta;
    public RowMetaInterface inputRowMeta;
    /** 无上游步骤时，首轮请求完成后置位，避免重复调用 API */
    public boolean standaloneRunCompleted;
}
