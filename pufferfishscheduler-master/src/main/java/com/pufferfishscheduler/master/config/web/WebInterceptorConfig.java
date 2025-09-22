package com.pufferfishscheduler.master.config.web;

import com.pufferfishscheduler.common.config.intercepter.TokenInterceptor;
import com.pufferfishscheduler.common.config.properties.InterceptorProperties;
import com.pufferfishscheduler.common.constants.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class WebInterceptorConfig implements WebMvcConfigurer {

    private final TokenInterceptor tokenInterceptor;

    @Value("${server.servlet.context-path:}")
    private String contextPath;

    @Autowired
    private InterceptorProperties interceptorProperties;

    @Autowired
    public WebInterceptorConfig(TokenInterceptor tokenInterceptor) {
        this.tokenInterceptor = tokenInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        if (interceptorProperties.getEnabled()) {
            // 处理排除路径
            List<String> fullExcludePaths = processExcludePaths();

            // 添加日志输出，方便调试
            log.info("=== 拦截器配置信息 ===");
            log.info("完整排除路径: " + fullExcludePaths);
            log.info("===================");

            registry.addInterceptor(tokenInterceptor)
                    .addPathPatterns("/**")
                    .excludePathPatterns(fullExcludePaths)
                    .order(1);
        }
    }

    private List<String> processExcludePaths() {
        return interceptorProperties.getExcludePaths().stream()
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .flatMap(path -> {
                    // 返回两个版本：原始路径和带context path的路径
                    List<String> paths = new ArrayList<>();

                    // 原始路径
                    paths.add(path);

                    // 带context path的路径
                    if (!path.startsWith("/")) {
                        path = "/" + path;
                    }
                    paths.add(contextPath + path);

                    return paths.stream();
                })
                .collect(Collectors.toList());
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins("*")
                .allowedMethods("PUT", "GET", "POST", "DELETE", "OPTIONS", "PATCH")
                .allowedHeaders("*")
                .exposedHeaders(
                        Constants.TOKEN_CONFIG.AUTHORIZATION,
                        Constants.TOKEN_CONFIG.TOKEN)
                .allowCredentials(true)
                .maxAge(3600);
    }
}