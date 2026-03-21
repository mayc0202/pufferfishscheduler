package com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity;

import com.pufferfishscheduler.common.exception.BusinessException;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FieldMapper {
    
    /**
     * 源字段名称
     */
    private String sourceFieldName;

    /**
     * 目标字段名称
     */
    private String targetFieldName;

    /**
     * 是否逻辑主键。1 - 是； 0 - 否。默认：否
     */
    private Boolean isLogicKey;

    /**
     * 验证字段映射是否合法
     */
    public void valid() {
        if (sourceFieldName == null || sourceFieldName.isEmpty()) {
            throw new BusinessException("源字段名称不能为空！");
        }
        if (targetFieldName == null || targetFieldName.isEmpty()) {
            throw new BusinessException("目标字段名称不能为空！");
        }
    }
}
