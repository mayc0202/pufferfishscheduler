package com.pufferfishscheduler.master.collect.rule.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.RuleProcessor;
import com.pufferfishscheduler.dao.mapper.RuleProcessorMapper;
import com.pufferfishscheduler.domain.vo.collect.ProcessorVo;
import com.pufferfishscheduler.domain.vo.collect.RuleProcessorVo;
import com.pufferfishscheduler.master.collect.rule.service.RuleProcessorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 规则处理器服务实现（只读）
 */
@Service("ruleProcessorService")
public class RuleProcessorServiceImpl implements RuleProcessorService {

    @Autowired
    private RuleProcessorMapper processorMapper;

    /**
     * 获取处理器列表
     */
    @Override
    public List<RuleProcessorVo> list() {
        LambdaQueryWrapper<RuleProcessor> qw = new LambdaQueryWrapper<>();
        qw.orderByAsc(RuleProcessor::getId);
        List<RuleProcessor> list = processorMapper.selectList(qw);
        List<RuleProcessorVo> result = new ArrayList<>(list.size());
        for (RuleProcessor s : list) {
            RuleProcessorVo one = new RuleProcessorVo();
            one.setId(s.getId());
            one.setProcessorName(s.getProcessorName());
            one.setParamsConfig(s.getParamsConfig());
            one.setProcessorClass(s.getProcessorClass());
            one.setStyleType(s.getStyleType());
            result.add(one);
        }
        return result;
    }

    /**
     * 获取处理器字典（id + name）

     */
    @Override
    public List<ProcessorVo> dict() {
        LambdaQueryWrapper<RuleProcessor> qw = new LambdaQueryWrapper<>();
        qw.orderByAsc(RuleProcessor::getId);
        List<RuleProcessor> list = processorMapper.selectList(qw);

        List<ProcessorVo> result = new ArrayList<>(list.size());
        for (RuleProcessor s : list) {
            ProcessorVo one = new ProcessorVo();
            one.setId(s.getId());
            one.setProcessorName(s.getProcessorName());
            result.add(one);
        }
        return result;
    }

    /**
     * 根据ID获取处理器详情
     */
    @Override
    public RuleProcessorVo detail(Integer id) {
        RuleProcessor one = processorMapper.selectById(id);
        if (one == null) {
            throw new BusinessException("处理器不存在，请刷新重试！");
        }
        RuleProcessorVo vo = new RuleProcessorVo();
        vo.setId(one.getId());
        vo.setProcessorName(one.getProcessorName());
        vo.setParamsConfig(one.getParamsConfig());
        vo.setProcessorClass(one.getProcessorClass());
        vo.setStyleType(one.getStyleType());
        return vo;
    }
}
