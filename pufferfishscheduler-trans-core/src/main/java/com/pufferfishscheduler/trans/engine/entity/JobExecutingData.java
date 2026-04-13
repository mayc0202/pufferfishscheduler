package com.pufferfishscheduler.trans.engine.entity;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JobExecutingData {
	private static Map<String, JobExecutingInfo> data=new ConcurrentHashMap<>();
	
	public static void put(String key,JobExecutingInfo jobExecutingInfo) {
		data.put(key, jobExecutingInfo);
	}
	
	public static JobExecutingInfo get(String key) {
		return data.get(key);
	}
	
	public static void remove(String key) {
		data.remove(key);
	}
	
	public static int size() {
		return data.size();
	}
}
