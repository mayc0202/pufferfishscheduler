package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.KettleFlowRepository;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * Kettle流程仓库表 Mapper
 *
 * @author Mayc
 * @date 2026-03-07 16:37:26
 */
@Mapper
public interface KettleFlowRepositoryMapper extends BaseMapper<KettleFlowRepository> {

        /**
         * 插入流程配置
         * 
         * @param flow 流程配置
         * @return 插入影响的行数
         */
        int insertFlow(KettleFlowRepository flow);

        /**
         * 更新流程配置的流程类型和配置
         * 
         * @param bizType     业务类型
         * @param bizObjectId 业务对象ID
         * @param flowContent 流程配置内容
         * @return 更新影响的行数
         */
        int updateFlow(@Param("flowType") String flowType,
                        @Param("flowContent") String flowContent,
                        @Param("flowJson") String flowJson,
                        @Param("bizType") String bizType,
                        @Param("bizObjectId") String bizObjectId);

        /**
         * 更新流程配置的主机名
         * 
         * @param hostName    主机名
         * @param bizType     业务类型
         * @param bizObjectId 业务对象ID
         * @return 更新影响的行数
         */
        int updateHostName(@Param("hostName") String hostName,
                        @Param("bizType") String bizType,
                        @Param("bizObjectId") String bizObjectId);

        /**
         * 根据业务类型和业务对象ID删除流程配置
         * 
         * @param bizType     业务类型
         * @param bizObjectId 业务对象ID
         * @return 删除影响的行数
         */
        int deleteFlow(@Param("bizType") String bizType,
                        @Param("bizObjectId") String bizObjectId);

        /**
         * 根据业务表关联ID查询流程配置
         * 
         * @param bizType     业务类型
         * @param bizObjectId 业务对象ID
         * @return 流程配置
         */
        KettleFlowRepository selectByBizTypeAndBizObjectId(@Param("bizType") String bizType,
                        @Param("bizObjectId") String bizObjectId);

        /**
         * 根据主机名查询所有流程
         * 
         * @param hostName 主机名
         * @return 流程配置列表
         */
        List<KettleFlowRepository> selectByHostName(@Param("hostName") String hostName);

}