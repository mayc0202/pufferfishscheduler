package com.pufferfishscheduler.cdc.kafka.entity;

/**
 * 实时数据同步任务运行状态
 * @author Mayc
 * @since 2026-03-16  09:28
 */
public class RealTimeSyncTaskStatus {
	
	public static final String 	TASK_STATUS_RUNNING = "RUNNING";

	public static final String 	TASK_STATUS_STOP = "STOP";

	public static final String 	TASK_STATUS_FAILURE = "FAILURE";

	public static final String 	TASK_STATUS_UNASSIGNED = "UNASSIGNED";

	/**
	 * 任务运行状态
	 * RUNNING - 运行中；STOP - 已停止；FAILURE - 异常；
	 */
	private String status;
	
	private String message;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	
}
