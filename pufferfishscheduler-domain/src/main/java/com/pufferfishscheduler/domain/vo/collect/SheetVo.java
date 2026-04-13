package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 工作表VO
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SheetVo {

    /**
     * 工作表名
     */
    private String sheetName;

    /**
     * 开始行
     */
    private int startRow;

    /**
     * 开始列
     */
    private int startColumn;
}
