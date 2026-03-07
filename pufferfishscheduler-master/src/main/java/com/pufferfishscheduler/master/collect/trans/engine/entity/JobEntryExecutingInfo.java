package com.pufferfishscheduler.master.collect.trans.engine.entity;

import java.util.Date;

/**
 * JobEntry执行结果
 * @author mayc
 *
 */
import lombok.Data;

@Data
public class JobEntryExecutingInfo {
	private String instanceId;
	private String entryName;
	private Date beginTime;
	private Date endTime;
	private boolean result;// true-执行成功；false-执行失败；
	private String reason;
	private long nrLinesRead;
	private long nrLinesWrite;
	private long nrLinesUpdated;
	private long nrLinesRejected;
	private long nrLinesError;
	private long nrLinesDeleted;
	private long nrReslutFiles;
	private long nrResultRow;
	private long nrLinesInput;
	private long nrLinesOutput;
}
