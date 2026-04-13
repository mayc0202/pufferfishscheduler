package com.pufferfishscheduler.plugin;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;


public class KafkaConsumerField {
    private static final Class<?> PKG = KafkaConsumerField.class;
    private Name kafkaName;

    @Injection(name = "OUTPUT_NAME")
    private String outputName;

    @Injection(name = "TYPE")
    private Type outputType;

    public KafkaConsumerField() {
        this.outputType = Type.String;
    }

    public KafkaConsumerField(KafkaConsumerField orig) {
        this.outputType = Type.String;
        this.kafkaName = orig.kafkaName;
        this.outputName = orig.outputName;
        this.outputType = orig.outputType;
    }

    public KafkaConsumerField(Name kafkaName, String outputName) {
        this(kafkaName, outputName, Type.String);
    }

    public KafkaConsumerField(Name kafkaName, String outputName, Type outputType) {
        this.outputType = Type.String;
        this.kafkaName = kafkaName;
        this.outputName = outputName;
        this.outputType = outputType;
    }

    public Name getKafkaName() {
        return this.kafkaName;
    }

    public void setKafkaName(Name kafkaName) {
        this.kafkaName = kafkaName;
    }

    public String getOutputName() {
        return this.outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }

    public Type getOutputType() {
        return this.outputType;
    }

    public void setOutputType(Type outputType) {
        this.outputType = outputType;
    }
    
    public enum Type {
        String("String", 2, StringSerializer.class, StringDeserializer.class),
        Integer("Integer", 5, LongSerializer.class, LongDeserializer.class),
        Binary("Binary", 8, ByteArraySerializer.class, ByteArrayDeserializer.class),
        Number("Number", 1, DoubleSerializer.class, DoubleDeserializer.class);

        private final String value;
        private final int valueMetaInterfaceType;
        private final Class kafkaSerializerClass;
        private final Class kafkaDeserializerClass;

        Type(String value, int valueMetaInterfaceType, Class kafkaSerializerClass, Class kafkaDeserializerClass) {
            this.value = value;
            this.valueMetaInterfaceType = valueMetaInterfaceType;
            this.kafkaSerializerClass = kafkaSerializerClass;
            this.kafkaDeserializerClass = kafkaDeserializerClass;
        }

        @Override // java.lang.Enum
        public String toString() {
            return this.value;
        }

        public int getValueMetaInterfaceType() {
            return this.valueMetaInterfaceType;
        }

        public Class getKafkaSerializerClass() {
            return this.kafkaSerializerClass;
        }

        public Class getKafkaDeserializerClass() {
            return this.kafkaDeserializerClass;
        }

        public static Type fromValueMetaInterface(ValueMetaInterface vmi) {
            if (vmi != null) {
                for (Type t : values()) {
                    if (vmi.getType() == t.getValueMetaInterfaceType()) {
                        return t;
                    }
                }
                throw new IllegalArgumentException(BaseMessages.getString(KafkaConsumerField.PKG, "KafkaConsumerField.Type.ERROR.NoValueMetaInterfaceMapping", new Object[]{vmi.getName(), valueOf(java.lang.String.valueOf(vmi.getType()))}));
            }
            return Type.String;
        }
    }

    public enum Name {
        KEY("key") {
            @Override 
            public void setFieldOnMeta(KafkaConsumerInputMeta meta, KafkaConsumerField field) {
                meta.setKeyField(field);
            }

            @Override 
            public KafkaConsumerField getFieldFromMeta(KafkaConsumerInputMeta meta) {
                return meta.getKeyField();
            }
        },
        MESSAGE("message") {
            @Override 
            public void setFieldOnMeta(KafkaConsumerInputMeta meta, KafkaConsumerField field) {
                meta.setMessageField(field);
            }

            @Override 
            public KafkaConsumerField getFieldFromMeta(KafkaConsumerInputMeta meta) {
                return meta.getMessageField();
            }
        },
        TOPIC("topic") {
            @Override 
            public void setFieldOnMeta(KafkaConsumerInputMeta meta, KafkaConsumerField field) {
                meta.setTopicField(field);
            }

            @Override 
            public KafkaConsumerField getFieldFromMeta(KafkaConsumerInputMeta meta) {
                return meta.getTopicField();
            }
        },
        PARTITION("partition") {
            public void setFieldOnMeta(KafkaConsumerInputMeta meta, KafkaConsumerField field) {
                meta.setPartitionField(field);
            }

            @Override 
            public KafkaConsumerField getFieldFromMeta(KafkaConsumerInputMeta meta) {
                return meta.getPartitionField();
            }
        },
        OFFSET(KafkaConsumerInputMeta.OFFSET_FIELD_NAME) {
            @Override 
            public void setFieldOnMeta(KafkaConsumerInputMeta meta, KafkaConsumerField field) {
                meta.setOffsetField(field);
            }

            @Override 
            public KafkaConsumerField getFieldFromMeta(KafkaConsumerInputMeta meta) {
                return meta.getOffsetField();
            }
        },
        TIMESTAMP(KafkaConsumerInputMeta.TIMESTAMP_FIELD_NAME) {
            @Override 
            public void setFieldOnMeta(KafkaConsumerInputMeta meta, KafkaConsumerField field) {
                meta.setTimestampField(field);
            }

            @Override 
            public KafkaConsumerField getFieldFromMeta(KafkaConsumerInputMeta meta) {
                return meta.getTimestampField();
            }
        };

        private final String name;

        public abstract void setFieldOnMeta(KafkaConsumerInputMeta kafkaConsumerInputMeta, KafkaConsumerField kafkaConsumerField);

        public abstract KafkaConsumerField getFieldFromMeta(KafkaConsumerInputMeta kafkaConsumerInputMeta);

        Name(String name) {
            this.name = name;
        }

        @Override // java.lang.Enum
        public String toString() {
            return this.name;
        }
    }
}