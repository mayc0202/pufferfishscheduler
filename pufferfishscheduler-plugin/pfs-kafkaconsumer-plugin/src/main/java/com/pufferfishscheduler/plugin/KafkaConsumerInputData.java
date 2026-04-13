package com.pufferfishscheduler.plugin;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorData;

public class KafkaConsumerInputData extends TransExecutorData implements StepDataInterface {
    RowMetaInterface outputRowMeta;
}