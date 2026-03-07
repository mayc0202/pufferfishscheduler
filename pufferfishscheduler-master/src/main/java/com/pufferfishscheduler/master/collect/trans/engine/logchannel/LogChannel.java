package com.pufferfishscheduler.master.collect.trans.engine.logchannel;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.lang3.StringUtils;

import com.pufferfishscheduler.master.collect.trans.engine.entity.LogText;
import com.pufferfishscheduler.master.collect.trans.engine.entity.StepStatus;

/**
 * 日志通道
 * 
 * @author Mayc
 *
 */
public class LogChannel implements Cloneable {
    // 类型常量
    public static final String TYPE_TRANS = "TRANS";
    public static final String TYPE_JOB = "JOB";
    
    // 状态常量
    public static final String STATUS_RUNNING = "R";
    public static final String STATUS_SUCCESS = "S";
    public static final String STATUS_FAILURE = "F";
    
    // 最大日志数量
    public static final int MAX_LOG_SIZE = 5000;
    
    private String type; // 类型：TRANS 或 JOB
    private String id; // 存放trans或者job的id
    private String name; // trans或job名称
    private List<LogText> logList; // 存放log日志
    private String status; // 状态：R-运行中；S-运行成功；F-执行失败
    private Map<String, StepStatus> stepStatusMap; // 步骤执行状态映射，key为步骤名称
    private Date finishDate;

    public LogChannel() {
        stepStatusMap = new HashMap<>();
        logList = new CopyOnWriteArrayList<>();
    }

    public LogChannel(String type, String id, String name) {
        this.type = type;
        this.id = id;
        this.name = name;
        stepStatusMap = new HashMap<>();
        logList = new CopyOnWriteArrayList<>();
    }

    /**
     * 清空日志和步骤状态
     */
    public void clear() {
        stepStatusMap.clear();
        logList.clear();
    }

    /**
     * 添加日志
     * @param logText 日志文本对象
     */
    public void addLog(LogText logText) {
        if (logText == null) {
            return;
        }
        
        // 控制日志大小
        if (logList.size() >= MAX_LOG_SIZE) {
            logList.remove(0);
        }
        logList.add(logText);
    }

    /**
     * 添加日志
     * @param status 状态
     * @param text 日志文本
     */
    public void addLog(String status, String text) {
        if (StringUtils.isBlank(text)) {
            return;
        }
        
        // 控制日志大小
        if (logList.size() >= MAX_LOG_SIZE) {
            logList.remove(0);
        }
        logList.add(new LogText(status, text));
    }

    /**
     * 添加或更新步骤状态
     * @param stepName 步骤名称
     * @param status 状态
     */
    public void addStepStatus(String stepName, String status) {
        if (StringUtils.isBlank(stepName)) {
            return;
        }
        stepStatusMap.put(stepName, new StepStatus(stepName, status));
    }

    /**
     * 添加或更新步骤状态
     * @param stepStatus 步骤状态对象
     */
    public void addStepStatus(StepStatus stepStatus) {
        if (stepStatus == null || StringUtils.isBlank(stepStatus.getStepName())) {
            return;
        }
        stepStatusMap.put(stepStatus.getStepName(), stepStatus);
    }

    /**
     * 获取步骤状态
     * @param stepName 步骤名称
     * @return 步骤状态
     */
    public StepStatus getStepStatus(String stepName) {
        return stepStatusMap.get(stepName);
    }

    /**
     * 获取所有步骤状态
     * @return 步骤状态列表
     */
    public List<StepStatus> getSteps() {
        return new ArrayList<>(stepStatusMap.values());
    }

    /**
     * 设置步骤状态列表
     * @param steps 步骤状态列表
     */
    public void setSteps(List<StepStatus> steps) {
        stepStatusMap.clear();
        if (steps != null) {
            for (StepStatus step : steps) {
                if (step != null && StringUtils.isNotBlank(step.getStepName())) {
                    stepStatusMap.put(step.getStepName(), step);
                }
            }
        }
    }

    /**
     * 克隆日志通道
     * @return 克隆后的日志通道
     */
    @Override
    public LogChannel clone() {
        try {
            LogChannel channel = (LogChannel) super.clone();
            channel.type = this.type;
            channel.id = this.id;
            channel.name = this.name;
            channel.status = this.status;
            channel.finishDate = this.finishDate != null ? (Date) this.finishDate.clone() : null;
            
            // 克隆日志列表
            channel.logList = new CopyOnWriteArrayList<>();
            for (LogText logText : this.logList) {
                if (logText != null) {
                    LogText clonedLog = new LogText(logText.getStatus(), logText.getText());
                    if (StringUtils.isNotBlank(clonedLog.getText())) {
                        clonedLog.setText(clonedLog.getText().replaceAll("Kettle", "Pufferfish"));
                    }
                    channel.addLog(clonedLog);
                }
            }
            
            // 克隆步骤状态
            channel.stepStatusMap = new HashMap<>();
            for (Map.Entry<String, StepStatus> entry : this.stepStatusMap.entrySet()) {
                if (entry.getValue() != null) {
                    StepStatus clonedStep = new StepStatus(entry.getValue().getStepName(), entry.getValue().getStatus());
                    channel.stepStatusMap.put(entry.getKey(), clonedStep);
                }
            }
            
            return channel;
        } catch (CloneNotSupportedException e) {
            // 处理克隆异常
            LogChannel channel = new LogChannel();
            channel.type = this.type;
            channel.id = this.id;
            channel.name = this.name;
            channel.status = this.status;
            channel.finishDate = this.finishDate;
            
            // 复制日志列表
            for (LogText logText : this.logList) {
                if (logText != null) {
                    LogText clonedLog = new LogText(logText.getStatus(), logText.getText());
                    if (StringUtils.isNotBlank(clonedLog.getText())) {
                        clonedLog.setText(clonedLog.getText().replaceAll("Kettle", "Pufferfish"));
                    }
                    channel.addLog(clonedLog);
                }
            }
            
            // 复制步骤状态
            for (StepStatus stepStatus : this.stepStatusMap.values()) {
                if (stepStatus != null) {
                    channel.addStepStatus(stepStatus);
                }
            }
            
            return channel;
        }
    }

    // Getter 和 Setter 方法
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<LogText> getLogList() {
        return logList;
    }

    public void setLogList(List<LogText> logList) {
        this.logList = logList != null ? new CopyOnWriteArrayList<>(logList) : new CopyOnWriteArrayList<>();
    }

    public Date getFinishDate() {
        return finishDate;
    }

    public void setFinishDate(Date finishDate) {
        this.finishDate = finishDate;
    }
}
