package com.pufferfishscheduler.master.collect.realtime.engine.kafka.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 *
 * @author Mayc
 * @since 2026-03-16  11:17
 */
@Data
@Configuration
public class KafkaDataProperties {

    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.connector}")
    private String connector;

    @Value("${app.dev-mode.enabled}")
    private Boolean devModel;

    @Value("${realtime.config.time.precision.mode}")
    private String timePrecisionMode;

    @Value("${realtime.config.decimal.handling.mode}")
    private String decimalHandlingMode;
}
