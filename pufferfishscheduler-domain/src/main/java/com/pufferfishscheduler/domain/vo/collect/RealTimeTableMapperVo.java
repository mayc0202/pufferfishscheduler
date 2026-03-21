package com.pufferfishscheduler.domain.vo.collect;

import java.util.List;

import lombok.Data;

/**
 * 实时数据同步表映射 VO（详情）
 */
@Data
public class RealTimeTableMapperVo {
    private Integer id;
    private Integer sourceTableId;
    private String sourceTableName;
    private Integer targetTableId;
    private String targetTableName;
    private Boolean deleteDataFlag;
    private Boolean parallelWriteFlag;
    private Integer parallelThreadNum;
    private String writeType;
    private Integer batchSize;
    /** 字段映射列表 */
    private List<RealTimeFieldMapperVo> fieldMappers;
}
