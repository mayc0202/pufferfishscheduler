package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Excel输出字段
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExcelOutputFieldVo {

    //名称
    private String name;

    //字段标题
    private String title;

    //类型
    private String type;

    //格式
    private String format;

    //超链
    private String hyperlinkField;
}
