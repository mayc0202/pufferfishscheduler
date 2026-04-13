package com.pufferfishscheduler.plugin;

import org.pentaho.di.core.injection.Injection;

/**
 * RabbitMQ 消费输出列定义（与前端 fieldList / Kafka 列模型对齐）。
 */
public class RabbitMqConsumerField {

    public enum Name {
        MESSAGE,
        ROUTING_KEY,
        MESSAGE_ID,
        DELIVERY_TAG,
        EXCHANGE,
        TIMESTAMP
    }

    public enum Type {
        String("String", 2),
        Integer("Integer", 5),
        Binary("Binary", 8),
        Number("Number", 1);

        private final String value;
        private final int valueMetaInterfaceType;

        Type(String value, int valueMetaInterfaceType) {
            this.value = value;
            this.valueMetaInterfaceType = valueMetaInterfaceType;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public int getValueMetaInterfaceType() {
            return this.valueMetaInterfaceType;
        }
    }

    @Injection(name = "RABBIT_NAME")
    private Name rabbitName;

    @Injection(name = "OUTPUT_NAME")
    private String outputName;

    @Injection(name = "TYPE")
    private Type outputType;

    public RabbitMqConsumerField() {
        this.outputType = Type.String;
    }

    public RabbitMqConsumerField(Name rabbitName, String outputName, Type outputType) {
        this.rabbitName = rabbitName;
        this.outputName = outputName;
        this.outputType = outputType != null ? outputType : Type.String;
    }

    public Name getRabbitName() {
        return rabbitName;
    }

    public void setRabbitName(Name rabbitName) {
        this.rabbitName = rabbitName;
    }

    public String getOutputName() {
        return outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }

    public Type getOutputType() {
        return outputType;
    }

    public void setOutputType(Type outputType) {
        this.outputType = outputType != null ? outputType : Type.String;
    }
}
