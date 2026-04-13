package com.pufferfishscheduler.plugin;

import org.apache.kafka.clients.producer.Producer;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class KafkaProducerOutputData extends BaseStepData implements StepDataInterface {
    Producer<Object, Object> kafkaProducer;
    int keyFieldIndex;
    int messageFieldIndex;
    boolean isOpen;
}
