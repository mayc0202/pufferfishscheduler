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
    /**
     * 任务实例ID
     */
    private String instanceId;
    /**
     * 任务名称
     */
    private String entryName;
    /**
     * 开始时间
     */
    private Date beginTime;
    /**
     * 结束时间
     */
    private Date endTime;
    /**
     * 执行结果
     * true-执行成功；false-执行失败
     */
    private boolean result;
    /**
     * 执行失败原因
     */
    private String reason;
    /**
     * 读取的行数
     */
    private long nrLinesRead;
    /**
     * 写入的行数
     */
    private long nrLinesWrite;
    /**
     * 更新的行数
     */
    private long nrLinesUpdated;
    /**
     * 拒绝的行数
     */
    private long nrLinesRejected;
    /**
     * 错误的行数
     */
    private long nrLinesError;
    /**
     * 删除的行数
     */
    private long nrLinesDeleted;
    /**
     * 结果文件数
     */
    private long nrReslutFiles;
    /**
     * 结果行数
     */
    private long nrResultRow;
    /**
     * 输入的行数
     */
    private long nrLinesInput;
    /**
     * 输出的行数
     */
    private long nrLinesOutput;
}
