package com.pufferfishscheduler.plugin;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class DataCleanStepData extends BaseStepData implements StepDataInterface {

    //输出行
    public RowMetaInterface outputRowMeta;
    //输入行
    public RowMetaInterface inputRowMeta;

    public DataCleanStepData() {

    }
}
