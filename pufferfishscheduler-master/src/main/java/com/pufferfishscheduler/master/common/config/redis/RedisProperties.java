package com.pufferfishscheduler.master.common.config.redis;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Redis配置属性
 *
 * @author Mayc
 * @since 2025-08-05  17:07
 */
@Slf4j
@Data
@Component
@ConfigurationProperties(prefix = "spring.redis")
public class RedisProperties {

    /**
     * Redis主机IP
     */
    private String host;
    /**
     * Redis主机端口
     */
    private int port;
    /**
     * Redis主机密码
     */
    private String password;
    /**
     * Redis数据库索引
     */
    private int database;
    /**
     * Redis连接超时时间
     */
    private Duration timeout;
    /**
     * Lettuce连接池配置
     */
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
