package com.pufferfishscheduler.plugin;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class RedisInputStepData extends BaseStepData implements StepDataInterface {
    public RowMetaInterface inputRowMeta;
    public RowMetaInterface outputRowMeta;
    public int inStreamNr;
}
