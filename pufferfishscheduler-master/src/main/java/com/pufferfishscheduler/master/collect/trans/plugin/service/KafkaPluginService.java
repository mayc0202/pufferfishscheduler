package com.pufferfishscheduler.master.collect.trans.plugin.service;

import com.pufferfishscheduler.common.node.Tree;

import java.util.List;

public interface KafkaPluginService {

    /**
     * 获取MQ数据源树形结构
     *
     * @return
     */
    List<Tree> mqDbTree();

    /**
     * 获取topic列表
     *
     * @return
     */
    List<String> topics(Integer id);
}
