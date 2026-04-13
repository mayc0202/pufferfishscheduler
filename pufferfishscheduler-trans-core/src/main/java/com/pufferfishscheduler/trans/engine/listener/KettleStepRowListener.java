package com.pufferfishscheduler.trans.engine.listener;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.logging.LogMessage;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.debug.StepDebugMeta;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassMeta;
import org.pentaho.di.trans.steps.writetolog.WriteToLogMeta;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.trans.engine.logchannel.LogChannelManager;

import lombok.extern.slf4j.Slf4j;

/**
 * kettle步骤行监听器
 * 用于监听转换作业中每个步骤的行读取和写入事件，记录行数据处理日志和统计信息。
 *
 * @author Mayc 
 */
@Slf4j
public class KettleStepRowListener implements RowListener {

    private boolean isReadData = false;
    private long lastDataVolume = 0L;
    private long currentDataVolume = 0L;
    private long lastTimestamp = 0L;
    
    // 统计频率（秒）
    private static final int STATS_FREQUENCY_SECONDS = 2;
    
    private final String logChannelId;
    private StepMetaDataCombi step;
    private StepDebugMeta stepDebugMeta;
    private int lastLogNumber = 0; // 记录最后处理的日志行号

    public KettleStepRowListener(String logChannelId) {
        this.logChannelId = logChannelId;
    }

    @Override
    public void rowReadEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
        if (!isReadData) {
            updateStatistics();
        }
    }

    @Override
    public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
        try {
            // 处理调试模式
            handleDebugMode(rowMeta, row);
            
            // 标记为已读取数据
            isReadData = true;
            
            // 处理写入日志步骤
            handleWriteToLogStep(rowMeta, row);
            
            // 处理Java脚本组件日志
            handleJavaScriptLogs();
            
            // 更新统计信息
            updateStatistics();
        } catch (Exception e) {
            log.error("处理行写入事件失败", e);
            throw new KettleStepException("处理行写入事件失败", e);
        }
    }

    @Override
    public void errorRowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
        // 可以在这里添加错误行处理逻辑
    }

    /**
     * 处理调试模式
     * @param rowMeta 行元数据
     * @param row 行数据
     * @throws KettleStepException 处理异常
     */
    private void handleDebugMode(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
        if (stepDebugMeta != null) {
            try {
                if (!stepDebugMeta.isReadingFirstRows() && !stepDebugMeta.isPausingOnBreakPoint()) {
                    // 经典预览模式，将行添加到缓冲区
                    stepDebugMeta.setRowBufferMeta(rowMeta);
                    stepDebugMeta.getRowBuffer().add(rowMeta.cloneRow(row));
                }
            } catch (KettleException e) {
                throw new KettleStepException("处理调试模式失败", e);
            }
        }
    }

    /**
     * 处理写入日志步骤
     * @param rowMeta 行元数据
     * @param row 行数据
     */
    private void handleWriteToLogStep(RowMetaInterface rowMeta, Object[] row) {
        if (step != null && step.meta instanceof WriteToLogMeta) {
            WriteToLogMeta writeToLogMeta = (WriteToLogMeta) step.meta;
            if (!writeToLogMeta.isLimitRows() || currentDataVolume <= writeToLogMeta.getLimitRowsNumber()) {
                StringBuilder message = new StringBuilder(
                        String.format("步骤\"%s\"日志第%s行。", step.stepMeta.getName(),
                                currentDataVolume == 0 ? 1 : currentDataVolume));
                
                for (String fieldName : writeToLogMeta.getFieldName()) {
                    int fieldIndex = rowMeta.indexOfValue(fieldName);
                    if (fieldIndex >= 0 && fieldIndex < row.length) {
                        message.append("字段").append(fieldName).append("值为").append(row[fieldIndex])
                                .append("; ");
                    }
                }

                LogChannelManager.get(logChannelId).addLog(Constants.EXECUTE_STATUS.SUCCESS, message.toString());
            }
        }
    }

    /**
     * 处理Java脚本组件日志
     */
    private void handleJavaScriptLogs() {
        if (step != null && step.meta instanceof UserDefinedJavaClassMeta) {
            processJavaScriptLogs();
        }
    }

    /**
     * 处理Java脚本组件日志
     */
    private void processJavaScriptLogs() {
        if (logChannelId == null || step == null) {
            return;
        }

        try {
            // 收集根通道及其所有子通道
            List<String> channelIds = new ArrayList<>(
                    LoggingRegistry.getInstance().getLogChannelChildren(logChannelId));

            // 拉取通道的日志
            int toLine = KettleLogStore.getLastBufferLineNr();
            List<KettleLoggingEvent> newLogs = KettleLogStore.getLogBufferFromTo(
                    channelIds,
                    false, // true 将所有通道数据都取出来
                    lastLogNumber,
                    toLine);

            // 按行拆分并发送
            for (KettleLoggingEvent event : newLogs) {
                if (event.getMessage() instanceof LogMessage) {
                    String[] parts = ((LogMessage) event.getMessage()).getMessage().split("\\r?\\n");
                    for (String part : parts) {
                        if (!part.isEmpty()) {
                            LogChannelManager.get(logChannelId)
                                    .addLog(Constants.EXECUTE_STATUS.SUCCESS,
                                            String.format("步骤[%s]脚本日志: %s",
                                                    step.stepMeta.getName(),
                                                    part));
                        }
                    }
                }
            }

            // 更新游标
            lastLogNumber = toLine;
        } catch (Exception e) {
            log.error("处理Java脚本日志失败", e);
        }
    }

    /**
     * 更新统计信息
     */
    private void updateStatistics() {
        currentDataVolume++;
        
        if (lastTimestamp == 0L) {
            lastTimestamp = System.currentTimeMillis();
            return;
        }

        long interval = System.currentTimeMillis() - lastTimestamp;
        if (interval >= STATS_FREQUENCY_SECONDS * 1000) {
            long rate = calculateProcessingRate(interval);
            
            // 更新统计数据
            lastTimestamp = System.currentTimeMillis();
            lastDataVolume = currentDataVolume;
            
            // 记录统计信息
            logStatistics(rate);
        }
    }

    /**
     * 计算处理速度
     * @param interval 时间间隔（毫秒）
     * @return 处理速度（条/秒）
     */
    private long calculateProcessingRate(long interval) {
        long processedRows = currentDataVolume - lastDataVolume;
        if (processedRows <= 0) {
            return 0;
        }
        
        long rate = (processedRows * 1000) / interval;
        return rate > 0 ? rate : 1;
    }

    /**
     * 记录统计信息
     * @param rate 处理速度（条/秒）
     */
    private void logStatistics(long rate) {
        if (step != null) {
            String message = String.format("步骤\"%s\"截至当前共处理记录数：%s，处理速度：%s 条/秒!", 
                    step.stepMeta.getName(), currentDataVolume, rate);
            log.info(message);
            LogChannelManager.get(logChannelId).addLog(Constants.EXECUTE_STATUS.SUCCESS, message);
        }
    }

    /**
     * 克隆监听器
     * @return 克隆后的监听器
     */
    public KettleStepRowListener clone() {
        KettleStepRowListener clonedListener = new KettleStepRowListener(logChannelId);
        clonedListener.step = this.step;
        clonedListener.stepDebugMeta = this.stepDebugMeta;
        clonedListener.lastLogNumber = this.lastLogNumber;
        return clonedListener;
    }

    public StepMetaDataCombi getStep() {
        return step;
    }

    public void setStep(StepMetaDataCombi step) {
        this.step = step;

        if (step != null && step.meta instanceof UserDefinedJavaClassMeta) {
            // 初始化logChannelId、lastLogNumber
            LogChannelInterface channel = step.step.getLogChannel();
            if (channel != null) {
                this.lastLogNumber = KettleLogStore.getLastBufferLineNr();
            }
        }
    }

    public StepDebugMeta getStepDebugMeta() {
        return stepDebugMeta;
    }

    public void setStepDebugMeta(StepDebugMeta stepDebugMeta) {
        this.stepDebugMeta = stepDebugMeta;
    }

    public long getCurrentDataVolume() {
        return currentDataVolume;
    }

    public String getLogChannelId() {
        return logChannelId;
    }
}
