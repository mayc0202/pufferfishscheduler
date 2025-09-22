package com.pufferfishscheduler.common.config.redis;

import io.lettuce.core.ClientOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @Author: yc
 * @CreateTime: 2025-05-25
 * @Description:
 * @Version: 1.0
 */
@Slf4j
@Configuration
public class RedisConfig {

    private final RedisProperties redisProperties;

    @Autowired
    public RedisConfig(RedisProperties redisProperties) {
        this.redisProperties = redisProperties;
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(redisProperties.getHost());
        config.setPort(redisProperties.getPort());
        config.setPassword(redisProperties.getPassword());

        GenericObjectPoolConfig<Object> poolConfig = new GenericObjectPoolConfig<>();
        RedisProperties.Lettuce.Pool poolProps = redisProperties.getLettuce().getPool();
        poolConfig.setMaxTotal(poolProps.getMaxActive());
        poolConfig.setMaxIdle(poolProps.getMaxIdle());
        poolConfig.setMinIdle(poolProps.getMinIdle());
        poolConfig.setMaxWait(poolProps.getMaxWait());

        LettucePoolingClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
                .poolConfig(poolConfig)
                .commandTimeout(redisProperties.getTimeout())
                .shutdownTimeout(redisProperties.getLettuce().getShutdownTimeout())
                .clientOptions(ClientOptions.builder()
                        .autoReconnect(true)
                        .pingBeforeActivateConnection(true)
                        .build())
                .build();

        LettuceConnectionFactory factory = new LettuceConnectionFactory(config, clientConfig);
        factory.setValidateConnection(true);
        return factory;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory());

        // Key序列化
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());

        // Value序列化（优化大对象处理）
        GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer() {
            @Override
            public byte[] serialize(Object object) throws SerializationException {
                try {
                    return super.serialize(object);
                } catch (Exception e) {
                    log.warn("Redis serialization error", e);
                    return null;  // 或返回空数组/默认值
                }
            }
        };

        template.setValueSerializer(serializer);
        template.setHashValueSerializer(serializer);

        // 重要性能参数
        template.setEnableTransactionSupport(false);  // 非事务模式
        template.setEnableDefaultSerializer(false);   // 禁用默认序列化
        template.afterPropertiesSet();

        return template;
    }
}