package com.pufferfishscheduler.plugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.FixedTimeStreamWindow;

public class KafkaConsumerInput extends BaseStreamStep implements StepInterface {
    private static final Class<?> PKG = KafkaConsumerInputMeta.class;

    public KafkaConsumerInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface) {
        KafkaConsumerInputMeta kafkaConsumerInputMeta = (KafkaConsumerInputMeta) stepMetaInterface;
        KafkaConsumerInputData kafkaConsumerInputData = (KafkaConsumerInputData) stepDataInterface;
        boolean superInit = super.init(kafkaConsumerInputMeta, kafkaConsumerInputData);
        if (!superInit) {
            logError(BaseMessages.getString(PKG, "KafkaConsumerInput.Error.InitFailed", new String[0]));
            return false;
        }
        try {
            kafkaConsumerInputData.outputRowMeta = kafkaConsumerInputMeta.getRowMeta(getStepname(), this);
        } catch (KettleStepException e) {
            this.log.logError(e.getMessage(), e);
        }
        Consumer consumer = kafkaConsumerInputMeta.getKafkaFactory().consumer(kafkaConsumerInputMeta, this::environmentSubstitute, kafkaConsumerInputMeta.getKeyField().getOutputType(), kafkaConsumerInputMeta.getMessageField().getOutputType());
        Set<String> topics = (Set) kafkaConsumerInputMeta.getTopics().stream().map(this::environmentSubstitute).collect(Collectors.toSet());
        if (StringUtils.isNotBlank(kafkaConsumerInputMeta.getPartition())) {
            String s = environmentSubstitute(kafkaConsumerInputMeta.getPartition());
            if (!s.contains("$")) {
                TopicPartition partition = new TopicPartition(kafkaConsumerInputMeta.getTopics().get(0), Integer.parseInt(s));
                consumer.assign(Collections.singletonList(partition));
            }
        } else {
            consumer.subscribe(topics);
        }
        this.source = new KafkaStreamSource(consumer, kafkaConsumerInputMeta, kafkaConsumerInputData, this.variables, this);
        KafkaStreamSource kafkaSource = (KafkaStreamSource) this.source;
        io.reactivex.functions.Consumer<Map.Entry<List<List<Object>>, Result>> afterWindow =
                kafkaConsumerInputMeta.isAutoCommit() ? entry -> { } : entry -> kafkaSource.commitOffsets(entry.getKey());
        this.window = new FixedTimeStreamWindow<>(
                getSubtransExecutor(),
                kafkaConsumerInputData.outputRowMeta,
                getDuration(),
                getBatchSize(),
                getParallelism(),
                afterWindow);
        return true;
    }
}