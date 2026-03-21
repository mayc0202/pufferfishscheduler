package com.pufferfishscheduler.ai;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 智能体应用
 *
 * @author Mayc
 * @since 2026-02-28  18:16
 */
@EnableDubbo
@EnableScheduling
@ComponentScan(basePackages = {"com.pufferfishscheduler"})
@MapperScan(value = "com.pufferfishscheduler.dao.mapper", annotationClass = org.apache.ibatis.annotations.Mapper.class) // 扫描所有Mapper接口
@SpringBootApplication
public class AgentApplication {
    public static void main(String[] args) {
        // 强制关闭 ZK 的 SASL 校验，解决 canonicalize 报错
        System.setProperty("zookeeper.sasl.client", "false");
        SpringApplication.run(AgentApplication.class, args);
    }
}
