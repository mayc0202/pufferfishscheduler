package com.pufferfishscheduler.master.collect.trans.config;

import com.pufferfishscheduler.dao.mapper.TransComponentMapper;
import com.pufferfishscheduler.trans.engine.DataCache;
import com.pufferfishscheduler.trans.engine.DataTransEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 数据转换引擎 Spring 配置（引擎实现见 pufferfishscheduler-trans-core）
 */
@Slf4j
@Configuration
public class SprintDataTransEngineConfiguration {

    @Autowired
    private TransComponentMapper transComponentMapper;

    @Bean
    public DataTransEngine dataEngine() {
        DataTransEngine dataEngine = new DataTransEngine();
        dataEngine.setTransComponentMapper(transComponentMapper);

        log.info("Init data trans engine (Kettle sync)...");
        try {
            dataEngine.init();
            DataCache.init();
            DataCache.setEnable(true);
            log.info("Finish data trans engine!");
        } catch (Exception e) {
            log.error("Failed to initialize data trans engine", e);
        }

        return dataEngine;
    }
}
