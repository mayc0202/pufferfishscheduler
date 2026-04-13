package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FieldSplitterVo {

    /**
     * 输出字段名称
     */
    private String fieldName;

    /**
     * 字段类型
     */
    private int fieldType;

    /**
     * 是否需要去除空格
     */
    private int fieldTrimType;

}
