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

		new Thread(new Runnable() {
			@Override
			public void run() {
				log.info("Init data trans engine!");

				try {
					// 初始化 Kettle 环境
					dataEngine.init();
					
					// 初始化数据缓存
					DataCache.init();
					DataCache.setEnable(true);

					log.info("Finish data trans engine!");
				} catch (Exception e) {
					log.error("Failed to initialize data trans engine", e);
					// 可以添加告警或其他处理逻辑
				}
			}
		}).start();

		return dataEngine;
	}

}
