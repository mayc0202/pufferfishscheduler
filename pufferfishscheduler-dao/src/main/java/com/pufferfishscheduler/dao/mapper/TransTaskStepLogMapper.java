package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.TransTaskStepLog;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface TransTaskStepLogMapper extends BaseMapper<TransTaskStepLog> {

    /**
     * 批量插入
     *
     * @param transTaskStepLogList
     */
    void insertBatch(List<TransTaskStepLog> transTaskStepLogList);
}
