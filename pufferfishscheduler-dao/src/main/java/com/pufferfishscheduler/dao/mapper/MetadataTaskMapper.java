package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.dao.entity.MetadataTask;
import com.pufferfishscheduler.domain.vo.metadata.MetadataTaskVo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.Map;


/**
 * metadata_task 表数据库访问层
 *
 * @author Mayc
 * @since 2025-11-25  15:13
 */
@Mapper
public interface MetadataTaskMapper extends BaseMapper<MetadataTask> {

    /**
     * 分页查询元数据任务列表
     */
    IPage<MetadataTaskVo> selectTaskList(IPage<MetadataTaskVo> page,
                                         @Param("params") Map<String, Object> params);

    /**
     * 查询任务详情
     */
    MetadataTaskVo selectTaskById(@Param("id") Integer id);
}
