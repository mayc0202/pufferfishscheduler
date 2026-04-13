package com.pufferfishscheduler.plugin;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * 构建 Kafka Producer；认证参数通过 {@link KafkaProducerOutputMeta#getConfig()} 合并进客户端配置。
 */
public class KafkaFactory {
    private final Function<Map<String, Object>, Producer<Object, Object>> producerFunction;

    public static KafkaFactory defaultFactory() {
        return new KafkaFactory(KafkaProducer::new);
    }

    KafkaFactory(Function<Map<String, Object>, Producer<Object, Object>> producerFunction) {
        this.producerFunction = producerFunction;
    }

    public Producer<Object, Object> producer(KafkaProducerOutputMeta meta, Function<String, String> variablesFunction) {
        return producer(meta, variablesFunction, KafkaTypes.Serde.String, KafkaTypes.Serde.String);
    }

    public Producer<Object, Object> producer(KafkaProducerOutputMeta meta, Function<String, String> variablesFunction,
            KafkaTypes.Serde keySerializerType, KafkaTypes.Serde msgSerializerType) {
        Function<String, String> substitute = variablesFunction.andThen(KafkaFactory::nullToEmpty);
        HashMap<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("bootstrap.servers", substitute.apply(meta.getBootstrapServers()));
        kafkaConfig.put("client.id", substitute.apply(meta.getClientId()));
        kafkaConfig.put("value.serializer", msgSerializerType.getKafkaSerializerClass());
        kafkaConfig.put("key.serializer", keySerializerType.getKafkaSerializerClass());
        meta.getConfig().entrySet().forEach(entry ->
                kafkaConfig.put(entry.getKey(), substitute.apply(entry.getValue())));
        return this.producerFunction.apply(kafkaConfig);
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
