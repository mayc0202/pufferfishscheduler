package com.pufferfishscheduler.common.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * 拦截器配置
 *
 * @author Mayc
 * @since 2025-09-22  13:50
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "security.interceptor")
public class InterceptorProperties {

    /**
     * 开启拦截器
     */
    private Boolean enabled;

    /**
     * 排除路径
     */
    private List<String> excludePaths = new ArrayList<>();


}
