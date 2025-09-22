package com.pufferfishscheduler.domain.vo.database;

import lombok.Data;

import java.util.List;

/**
 * 资源树
 */
@Data
public class ResourceTreeVo {

    /**
     * 目标标识
     */
    private String label;

    /**
     * 目录名
     */
    private String value;

    /**
     * 子级
     */
    private List<ResourceTreeVo> children;
}
