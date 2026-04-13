package com.pufferfishscheduler.worker.task.trans.engine;

import com.pufferfishscheduler.worker.task.trans.service.TransTaskLogService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 数据转换引擎配置
 * 
 * @author Mayc
 *
 */
@Slf4j
@Configuration
public class SprintDataTransEngineConfiguration {

    @Value("${log.number.max:1000}")
    private Integer maxLogNumber;

    @Autowired
    private TransTaskLogService transTaskLogService;

	@Bean
	public DataTransEngine dataEngine() {

		DataTransEngine dataEngine = new DataTransEngine();
        dataEngine.setMaxLogNumber(maxLogNumber);
        dataEngine.setTransTaskLogService(transTaskLogService);

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
