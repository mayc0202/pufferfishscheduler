package com.pufferfishscheduler.plugin;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class RedisOutputStepData extends BaseStepData implements StepDataInterface {
    public RowMetaInterface inputRowMeta;
    public int keyNr;
    public int valueNr;
    public RowMetaInterface outputRowMeta;
}
