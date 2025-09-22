package com.pufferfishscheduler.common.config.redis;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * @author Mayc
 * @since 2025-08-05  17:07
 */
@Slf4j
@Data
@Component
@ConfigurationProperties(prefix = "spring.redis")
public class RedisProperties {

    // ip
    private String host;
    // port
    private int port;
    // pwd
    private String password;
    // database
    private int database;
    // timeout
    private Duration timeout;
    // lettuce
    private Lettuce lettuce;

    @Data
    public static class Lettuce {
        private Pool pool;
        private Duration shutdownTimeout;

        @Data
        public static class Pool {
            private int maxActive;
            private int maxIdle;
            private int minIdle;
            private Duration maxWait;
        }
    }
}
