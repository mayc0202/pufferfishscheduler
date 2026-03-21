package com.pufferfishscheduler.domain.vo.collect;

import java.util.Date;
import java.util.List;

import com.alibaba.fastjson2.annotation.JSONField;

import lombok.Data;

/**
 * 实时数据同步任务VO
 * 
 * @author Mayc
 * @since 2026-03-13 17:44
 */
@Data
public class RealTimeTaskVo {
    /**
     * 任务ID
     */
    private Integer taskId;

    /**
     * 任务名称
     */
    private String taskName;

    /**
     * 任务状态
     */
    private String taskStatus;

    private String taskStatusTxt;

    /**
     * 源数据库ID
     */
    private Integer sourceDbId;

    /**
     * 源数据库名称
     */
    private String sourceDbName;

    /**
     * 源表ID
     */
    private Integer sourceTableId;

     /**
      * 源表名称
      */
    private String sourceTableName;

    /**
     * 目标数据库ID
     */
    private Integer targetDbId;

    /**
     * 目标数据库名称
     */
    private String targetDbName;

    /**
     * 目标表ID
     */
    private Integer targetTableId;

    /** 
     * 目标表名称
     */
    private String targetTableName;

    /**
     * 引擎类型
     */
    private String engineType;

    private String engineTypeTxt;

    /**
     * 原因
     */
    private String reason;

    /**
     * 创建时间
     */
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date createdTime;

    private String createdTimeTxt;

    /**
     * 表映射列表（详情接口返回）
     */
    private List<RealTimeTableMapperVo> tableMappers;
}
