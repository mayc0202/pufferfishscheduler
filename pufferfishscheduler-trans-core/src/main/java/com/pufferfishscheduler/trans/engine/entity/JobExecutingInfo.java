package com.pufferfishscheduler.trans.engine.entity;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import lombok.Data;

@Data
public class JobExecutingInfo {
	private String instanceId;
	private String taskFlowLogId;
	private String jobName;
	private String parentInstanceId;
	private String businessType;
	private String businessNo;
	private Date beginTime;
	private Date endTime;
	private Boolean result;// true-执行成功；false-执行失败；
	private String reason;
	private Long nrLinesRead;
	private Long nrLinesWrite;
	private Long nrLinesUpdated;
	private Long nrLinesRejected;
	private Long nrLinesError;
	private Long nrLinesDeleted;

	private Long nrReslutFiles;
	private Long nrResultRow;

	private Long nrLinesInput;
	private Long nrLinesOutput;

	private String executingType;// manual-手工,timing-定时
	private String parameters;
	private String flowImage;// 流程图片
	private String dataJobId;// 业务表ID

	private String executingServer;
	private String executingUser;
	private String clientNodeCode;

	private String logText;

	private Boolean finished = false;

	private List<JobEntryExecutingInfo> jobEntryExcutinginfos = new ArrayList<JobEntryExecutingInfo>();

}
