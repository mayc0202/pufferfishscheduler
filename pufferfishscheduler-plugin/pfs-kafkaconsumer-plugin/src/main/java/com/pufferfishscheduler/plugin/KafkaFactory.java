package com.pufferfishscheduler.plugin;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 构建 Kafka Consumer（无 Pentaho Hadoop Shim 依赖；SASL/SSL 通过 Meta 的 advancedConfig 传入）。
 */
public class KafkaFactory {
    private final Function<Map<String, Object>, Consumer> consumerFunction;

    public static KafkaFactory defaultFactory() {
        return new KafkaFactory(KafkaConsumer::new);
    }

    KafkaFactory(Function<Map<String, Object>, Consumer> consumerFunction) {
        this.consumerFunction = consumerFunction;
    }

    public Consumer consumer(KafkaConsumerInputMeta meta, Function<String, String> variablesFunction) {
        return consumer(meta, variablesFunction, KafkaConsumerField.Type.String, KafkaConsumerField.Type.String);
    }

    public Consumer consumer(KafkaConsumerInputMeta meta, Function<String, String> variablesFunction,
            KafkaConsumerField.Type keyDeserializerType, KafkaConsumerField.Type msgDeserializerType) {
        HashMap<String, Object> kafkaConfig = new HashMap<>();
        Function<String, String> substitute = variablesFunction.andThen(KafkaFactory::nullToEmpty);
        kafkaConfig.put("bootstrap.servers", substitute.apply(meta.getBootstrapServers()));
        kafkaConfig.put("group.id", substitute.apply(meta.getConsumerGroup()));
        kafkaConfig.put("value.deserializer", msgDeserializerType.getKafkaDeserializerClass());
        kafkaConfig.put("key.deserializer", keyDeserializerType.getKafkaDeserializerClass());
        kafkaConfig.put("enable.auto.commit", Boolean.valueOf(meta.isAutoCommit()));
        meta.getConfig().entrySet().forEach(entry ->
                kafkaConfig.put(entry.getKey(), substitute.apply(entry.getValue())));
        return this.consumerFunction.apply(kafkaConfig);
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
