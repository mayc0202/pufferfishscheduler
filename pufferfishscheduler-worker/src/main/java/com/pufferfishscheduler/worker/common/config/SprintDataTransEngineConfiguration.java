package com.pufferfishscheduler.worker.common.config;

import com.pufferfishscheduler.trans.engine.DataCache;
import com.pufferfishscheduler.trans.engine.DataTransEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Worker 数据转换引擎 Spring 配置（引擎实现见 pufferfishscheduler-trans-core）
 */
@Slf4j
@Configuration
public class SprintDataTransEngineConfiguration {

    @Bean
    public DataTransEngine dataEngine() {
        DataTransEngine dataEngine = new DataTransEngine();

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
