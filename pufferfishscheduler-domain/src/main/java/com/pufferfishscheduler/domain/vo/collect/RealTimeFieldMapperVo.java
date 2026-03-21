package com.pufferfishscheduler.domain.vo.collect;

import lombok.Data;

/**
 * 实时数据同步字段映射 VO（详情）
 */
@Data
public class RealTimeFieldMapperVo {
    private Integer id;
    private Integer sourceFieldId;
    private String sourceFieldName;
    private Integer targetFieldId;
    private String targetFieldName;
}
