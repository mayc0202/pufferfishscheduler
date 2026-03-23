package com.pufferfishscheduler.dao.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.RtTask;

/**
 * RTTask的Mapper接口
 * 实时同步任务表
 */
public interface RtTaskMapper extends BaseMapper<RtTask> {

    /**
     * 根据源数据库ID和目标数据库ID查询任务
     * @param sourceDbId 源数据库ID
     * @param targetDbId 目标数据库ID
     * @return 任务信息
     */
    RtTask selectBySourceAndTarget(@Param("sourceDbId") Integer sourceDbId, @Param("targetDbId") Integer targetDbId);

    /**
     * 根据任务状态查询任务列表
     * @param status 任务状态
     * @return 任务列表
     */
    List<RtTask> selectByStatus(String status);

    /**
     * 根据业务主题ID查询任务列表
     * @param groupId 业务主题ID
     * @return 任务列表
     */
    List<RtTask> selectByGroupId(String groupId);

    /**
     * 更新任务状态
     * @param id 任务ID
     * @param status 新状态
     * @param reason 异常原因（可选）
     * @return 更新结果
     */
    int updateTaskStatus(@Param("id") Integer id, @Param("status") String status, @Param("reason") String reason);

    /**
     * 更新运行时配置
     * @param id 任务ID
     * @param runtimeConfig 运行时配置
     * @return 更新结果
     */
    int updateRuntimeConfig(@Param("id") Integer id, @Param("runtimeConfig") String runtimeConfig);

    /**
     * 批量更新任务状态
     * @param status 新状态
     * @param ids 任务ID列表
     * @return 更新结果
     */
    int batchUpdateStatus(@Param("status") String status, @Param("ids") java.util.List<Integer> ids);

    /**
     * 获取运行中的任务列表
     * @return 运行中任务列表
     */
    java.util.List<RtTask> selectRunningTasks();

    /**
     * 获取需要心跳检查的任务列表
     * @return 心跳检查任务列表
     */
    java.util.List<RtTask> selectHeartbeatTasks();

}
