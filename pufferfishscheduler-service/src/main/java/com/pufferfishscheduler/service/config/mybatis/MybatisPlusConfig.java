package com.pufferfishscheduler.service.config.mybatis;

import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Mybatis plus configuration
 * MyBatis Plus 3.5.x版本默认包含分页插件，无需额外配置
 */
@Configuration
public class MybatisPlusConfig {

    /**
     * MyBatis Plus interceptor configuration
     * MyBatis Plus 3.5.x版本默认已经包含了分页插件
     */
    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        return new MybatisPlusInterceptor();
    }
}