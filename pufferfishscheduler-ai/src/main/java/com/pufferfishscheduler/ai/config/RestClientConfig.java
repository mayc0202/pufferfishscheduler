package com.pufferfishscheduler.ai.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;


@Configuration
public class RestClientConfig {

    @Bean
    public RestClient restClient() {
        // 配置请求工厂
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(60000);      // 连接超时 60秒
        factory.setReadTimeout(120000);        // 读取超时 120秒

        return RestClient.builder()
                .requestFactory(factory)
                .build();
    }
}