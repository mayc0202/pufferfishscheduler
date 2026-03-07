package com.pufferfishscheduler.domain.vo.collect;

import java.util.List;

import lombok.Data;

/**
 * 预览数据VO
 */
@Data
public class PreviewVo {
    /**
     * 字段列表
     */
    private String[] fieldList;
    
    /**
     * 数据列表
     */
    private List<Object[]> dataList;

}
