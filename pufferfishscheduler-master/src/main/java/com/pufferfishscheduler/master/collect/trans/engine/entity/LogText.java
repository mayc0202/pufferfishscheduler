package com.pufferfishscheduler.master.collect.trans.engine.entity;


import java.text.SimpleDateFormat;
import java.util.Date;

import lombok.Data;

/**
 * 日志文本
 */
@Data
public class LogText {
    private String status;
	private String text;

    public LogText(String status, String text) {
		this.status = status;
		this.text = formatText(text);
	}
	
	private String formatText(String text) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return String.format("%s %s", sdf.format(new Date()),text);
	}
}
