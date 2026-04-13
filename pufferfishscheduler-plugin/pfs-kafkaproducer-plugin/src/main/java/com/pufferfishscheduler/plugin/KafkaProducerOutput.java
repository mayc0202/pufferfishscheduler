package com.pufferfishscheduler.plugin;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Kafka 输出步骤：按行将 key/message 字段发送到指定 topic。
 */
public class KafkaProducerOutput extends BaseStep implements StepInterface {

    private KafkaProducerOutputMeta meta;
    private KafkaProducerOutputData data;
    private KafkaFactory kafkaFactory;

    public KafkaProducerOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
            Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        setKafkaFactory(KafkaFactory.defaultFactory());
    }

    void setKafkaFactory(KafkaFactory factory) {
        this.kafkaFactory = factory;
    }

    @Override
    public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface) {
        super.init(stepMetaInterface, stepDataInterface);
        this.meta = (KafkaProducerOutputMeta) stepMetaInterface;
        this.data = (KafkaProducerOutputData) stepDataInterface;
        return true;
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        ProducerRecord<Object, Object> producerRecord;
        Object[] r = getRow();
        if (r == null) {
            setOutputDone();
            if (this.data.kafkaProducer != null) {
                this.data.kafkaProducer.close();
            }
            return false;
        }
        if (this.first) {
            this.data.keyFieldIndex = getInputRowMeta().indexOfValue(environmentSubstitute(this.meta.getKeyField()));
            this.data.messageFieldIndex = getInputRowMeta().indexOfValue(environmentSubstitute(this.meta.getMessageField()));
            ValueMetaInterface keyValueMeta = getInputRowMeta().getValueMeta(this.data.keyFieldIndex);
            ValueMetaInterface msgValueMeta = getInputRowMeta().getValueMeta(this.data.messageFieldIndex);
            this.data.kafkaProducer = this.kafkaFactory.producer(this.meta, this::environmentSubstitute,
                    KafkaTypes.Serde.fromValueMetaInterface(keyValueMeta),
                    KafkaTypes.Serde.fromValueMetaInterface(msgValueMeta));
            this.data.isOpen = true;
            this.first = false;
        }
        if (!this.data.isOpen) {
            return false;
        }
        if (this.data.keyFieldIndex < 0 || r[this.data.keyFieldIndex] == null
                || StringUtil.isEmpty(r[this.data.keyFieldIndex].toString())) {
            producerRecord = new ProducerRecord<>(environmentSubstitute(this.meta.getTopic()), r[this.data.messageFieldIndex]);
        } else {
            producerRecord = new ProducerRecord<>(environmentSubstitute(this.meta.getTopic()),
                    r[this.data.keyFieldIndex], r[this.data.messageFieldIndex]);
        }
        this.data.kafkaProducer.send(producerRecord);
        incrementLinesOutput();
        putRow(getInputRowMeta(), r);
        return true;
    }

    @Override
    public void stopRunning(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface) {
        if (this.data != null && this.data.kafkaProducer != null && this.data.isOpen) {
            this.data.isOpen = false;
            this.data.kafkaProducer.flush();
            this.data.kafkaProducer.close();
        }
    }
}
