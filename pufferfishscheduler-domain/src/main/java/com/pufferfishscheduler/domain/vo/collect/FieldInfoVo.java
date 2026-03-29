package com.pufferfishscheduler.domain.vo.collect;

import lombok.Data;

/**
 * 字段信息
 */
@Data
public class FieldInfoVo {
    /**
     * 字段名称
     */
    private String name;

    /**
     * 类型
     */
    private Integer type;

    /**
     * 长度
     */
    private Integer length;

    /**
     * 精度
     */
    private Integer precision;

}
