package com.pufferfishscheduler.plugin;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.util.concurrent.LinkedBlockingQueue;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class RabbitMqProducerOutputData extends BaseStepData implements StepDataInterface {

    Connection connection;
    Channel channel;
    boolean open;
    int messageFieldIndex = -1;
    int routingKeyFieldIndex = -1;

    /** mandatory+returns 时由 broker 退回的通知 */
    final LinkedBlockingQueue<String> returnSignals = new LinkedBlockingQueue<>();

    public RabbitMqProducerOutputData() {
        super();
    }
}
