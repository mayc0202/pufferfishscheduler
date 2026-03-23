package com.pufferfishscheduler.worker.task.trans.engine.entity;


import lombok.Data;

import java.text.SimpleDateFormat;
import java.util.Date;

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
