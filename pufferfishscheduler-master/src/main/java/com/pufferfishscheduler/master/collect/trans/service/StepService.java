package com.pufferfishscheduler.master.collect.trans.service;

import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.domain.form.collect.PreviewForm;
import com.pufferfishscheduler.domain.vo.collect.PreviewVo;
import com.pufferfishscheduler.trans.engine.entity.TransParam;
import com.pufferfishscheduler.trans.engine.logchannel.LogChannel;
import org.pentaho.di.trans.TransMeta;

import java.util.List;

/**
 * 转换流步骤服务接口
 * 注意：该接口不支持事务管理，统一由调用方负责管理事务
 */
public interface StepService {

    /**
     * 更新数据清洗规则配置
     *
     * @param ruleConfig 数据清洗规则配置
     * @return 更新后的数据清洗规则配置
     */
    String updateDataCleanRuleConfig(String ruleConfig);

    /**
     * 构建转换元数据
     *
     * @param flowId   流程ID
     * @param config   配置信息
     * @param validate 是否验证
     * @return 转换元数据
     */
    TransMeta buildTransMeta(Integer flowId, String config, boolean validate);

    /**
     * 设置转换流参数
     *
     * @param flowId   流程ID
     * @param transMeta 转换元数据
     * @param params    转换流参数
     */
    void setParams(Integer flowId, TransMeta transMeta, List<TransParam> params);

    /**
     * 转换前处理
     *
     * @param flowId   流程ID
     * @param config   配置信息
     * @param params   转换流参数
     */
    void beforeTrans(Integer flowId, String config, List<TransParam> params);

    /**
     * 同步执行转换流
     *
     * @param transFlow 转换流
     * @param logChannel 日志通道
     */
    void syncExecute(TransFlow transFlow, LogChannel logChannel);

    /**
     * 停止转换流
     *
     * @param id 转换流id
     */
    void stop(Integer id);

    /**
     * 调试解析转换流配置
     *
     * @param config 转换流配置
     */
    void debugParseConfig(String config);

    /**
     * 预览转换流数据
     *
     * @param form 转换流配置表单
     * @return 预览数据
     */
    PreviewVo preview(PreviewForm form);

    /**
     * 获取转换流字段流
     *
     * @param flowId   转换流id
     * @param config   转换流配置
     * @param stepName 转换流步骤名称
     * @param type     字段类型
     * @return 字段流
     */
    String[] getFieldStream(Integer flowId, String config, String stepName, Integer type);

    /**
     * 检查转换流状态
     *
     * @param id 转换流id
     * @return 转换流状态
     */
    Boolean checkTransStatus(Integer id);

    /**
     * 获取转换流程运行日志
     *
     * @param id 转换流id
     * @return 转换流程运行日志
     */
    LogChannel getProcessLog(Integer id);

    /**
     * 展示转换流程图片
     *
     * @param transFlow 转换流程对象
     * @return 转换流程图片base64编码
     */
    String showTransImg(TransFlow transFlow);

}
