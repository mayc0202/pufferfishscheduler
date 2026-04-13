package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 基础字段VO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BaseFieldVo {

    /**
     * 名称
     */
    private String name;

    /**
     * 类型
     */
    private int type;

    /**
     * 长度
     */
    private String length;

    /**
     * 精度
     */
    private String precision;

    /**
     * 去空格类型
     */
    private int trimtype;

    /**
     * 格式
     */
    private String format;

    /**
     * 货币符号
     */
    private String currencySymbol;

    /**
     * 小数符号
     */
    private String decimalSymbol;

    /**
     * 千分位符号
     */
    private String groupSymbol;

    private boolean repeat;
}
