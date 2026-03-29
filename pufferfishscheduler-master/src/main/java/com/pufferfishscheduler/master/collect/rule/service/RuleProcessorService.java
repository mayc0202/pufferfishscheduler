package com.pufferfishscheduler.master.collect.rule.service;

import com.pufferfishscheduler.domain.vo.collect.ProcessorVo;
import com.pufferfishscheduler.domain.vo.collect.RuleProcessorVo;

import java.util.List;

/**
 * 规则处理器服务
 */
public interface RuleProcessorService {

    /**
     * 获取处理器列表
     */
    List<RuleProcessorVo> list();

    /**
     * 获取处理器字典（id + name）
     */
    List<ProcessorVo> dict();

    /**
     * 根据ID获取处理器详情
     */
    RuleProcessorVo detail(Integer id);
}
