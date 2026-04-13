package com.pufferfishscheduler.plugin;

import java.util.concurrent.BlockingQueue;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class RabbitMqConsumerInputData extends BaseStepData implements StepDataInterface {

    RowMetaInterface outputRowMeta;
    RowMetaInterface inputRowMeta;

    BlockingQueue<RabbitMqConsumerInput.QueuedDelivery> deliveryQueue;

    volatile boolean standaloneRunCompleted;

    long messagesEmitted;

    public RabbitMqConsumerInputData() {
        super();
    }
}
