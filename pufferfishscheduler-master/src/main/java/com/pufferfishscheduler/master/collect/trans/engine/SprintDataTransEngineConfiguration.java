package com.pufferfishscheduler.master.collect.trans.engine;

import com.pufferfishscheduler.dao.mapper.TransComponentMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

	@Autowired
	private TransComponentMapper transComponentMapper;

	@Bean
	public DataTransEngine dataEngine() {

		DataTransEngine dataEngine = new DataTransEngine();
		dataEngine.setTransComponentMapper(transComponentMapper);

		log.info("Init data trans engine (Kettle sync)...");
		try {
			// 必须在任意 buildTransMeta / PluginRegistry 使用之前完成，否则步骤 pluginId 解析失败会导致 flow_content 中 <type/> 为空
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
