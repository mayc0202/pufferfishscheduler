package com.pufferfishscheduler.master;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 *
 * @author Mayc
 * @since 2025-09-21  02:37
 */
@EnableScheduling // 开启定时任务
@SpringBootApplication
@ComponentScan(basePackages = {"com.pufferfishscheduler"})
@MapperScan("com.pufferfishscheduler.dao.mapper") // 扫描所有Mapper接口
public class MasterApplication {
    public static void main(String[] args) {
        SpringApplication.run(MasterApplication.class, args);
    }
}
