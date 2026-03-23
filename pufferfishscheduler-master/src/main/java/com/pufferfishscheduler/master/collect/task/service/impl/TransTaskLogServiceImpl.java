package com.pufferfishscheduler.master.collect.task.service.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.DateUtil;
import com.pufferfishscheduler.dao.mapper.TransTaskLogMapper;
import com.pufferfishscheduler.domain.vo.collect.TransTaskLogVo;
import com.pufferfishscheduler.domain.vo.collect.TransTaskVo;
import com.pufferfishscheduler.domain.vo.dict.Dict;
import com.pufferfishscheduler.master.collect.task.service.TransTaskLogService;
import com.pufferfishscheduler.master.common.dict.service.DictService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 转换任务日志服务实现类
 *
 * @author Mayc
 * @since 2026-03-23  16:05com.pufferfishscheduler.master.collect.task.service.
 */
@Slf4j
@Service
public class TransTaskLogServiceImpl implements TransTaskLogService {

    @Autowired
    private DictService dictService;

    @Autowired
    private TransTaskLogMapper taskLogMapper;

    /**
     * 分页查询
     *
     * @param taskName  任务名称
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @param status    状态
     * @param pageNo    页码
     * @param pageSize  每页数量
     * @return
     */
    @Override
    public IPage<TransTaskLogVo> list(String taskName, String startTime, String endTime, String status, Integer pageNo, Integer pageSize) {

        Map<String, Object> params = new HashMap<>(8);
        if (StringUtils.isNotBlank(taskName)) {
            params.put("taskName", taskName.trim());
        }

        if (StringUtils.isNotBlank(status)) {
            params.put("status", status);
        }

        if (null != startTime) {
            params.put("startTime", startTime);
        }

        if (null != endTime) {
            params.put("endTime", endTime);
        }

        Page<TransTaskLogVo> page = new Page<>(pageNo, pageSize);
        IPage<TransTaskLogVo> result = taskLogMapper.selectTaskLogList(page, params);
        result.getRecords().forEach(this::fillDictText);
        return result;
    }

    /**
     * 查询转换任务日志详情
     *
     * @param id 日志ID
     * @return 转换任务日志VO
     */
    @Override
    public TransTaskLogVo detail(String id) {
        TransTaskLogVo detail = taskLogMapper.detail(id);
        if (null == detail) {
            throw new BusinessException("日志不存在");
        }
        fillDictText(detail);
        return detail;
    }

    /**
     * 填充字典文本
     */
    private void fillDictText(TransTaskLogVo vo) {
        vo.setStatusTxt(dictService.getDictItemValue(Constants.DICT.EXECUTE_STATUS, vo.getStatus()));
        vo.setExecutingTypeTxt(dictService.getDictItemValue(Constants.DICT.SCHEDULE_TYPE, vo.getExecutingType()));
        vo.setStartTimeTxt(DateUtil.formatDateTime(vo.getStartTime()));
        vo.setEndTimeTxt(DateUtil.formatDateTime(vo.getEndTime()));
    }
}
