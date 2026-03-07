package com.pufferfishscheduler.master.collect.trans.service;

import com.pufferfishscheduler.domain.vo.collect.ComponentNodeVo;

import java.util.List;

/**
 * 转换组件 Service
 *
 * @author Mayc
 * @since 2026-03-02  22:49
 */
public interface TransComponentService {

    /**
     * 获取组件树结构
     *
     * @return 组件树节点列表
     */
    List<ComponentNodeVo> getComponentTree();
}
