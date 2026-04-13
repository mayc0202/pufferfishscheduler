package com.pufferfishscheduler.plugin;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pentaho.di.core.row.ValueMetaInterface;

/**
 * 与 Kafka 客户端序列化器对应的 Kettle 值类型（与 consumer 插件中字段类型约定一致）。
 */
public final class KafkaTypes {

    private KafkaTypes() {
    }

    public enum Serde {
        String("String", 2, StringSerializer.class, StringDeserializer.class),
        Integer("Integer", 5, LongSerializer.class, LongDeserializer.class),
        Binary("Binary", 8, ByteArraySerializer.class, ByteArrayDeserializer.class),
        Number("Number", 1, DoubleSerializer.class, DoubleDeserializer.class);

        private final String value;
        private final int valueMetaInterfaceType;
        private final Class<?> kafkaSerializerClass;
        private final Class<?> kafkaDeserializerClass;

        Serde(String value, int valueMetaInterfaceType, Class<?> kafkaSerializerClass, Class<?> kafkaDeserializerClass) {
            this.value = value;
            this.valueMetaInterfaceType = valueMetaInterfaceType;
            this.kafkaSerializerClass = kafkaSerializerClass;
            this.kafkaDeserializerClass = kafkaDeserializerClass;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public Class<?> getKafkaSerializerClass() {
            return this.kafkaSerializerClass;
        }

        public Class<?> getKafkaDeserializerClass() {
            return this.kafkaDeserializerClass;
        }

        public static Serde fromValueMetaInterface(ValueMetaInterface vmi) {
            if (vmi != null) {
                for (Serde t : values()) {
                    if (vmi.getType() == t.valueMetaInterfaceType) {
                        return t;
                    }
                }
                throw new IllegalArgumentException("Unsupported value meta type for Kafka: " + vmi.getName()
                        + " type=" + vmi.getType());
            }
            return String;
        }
    }
}
