package com.pufferfishscheduler.master.collect.trans.plugin.service;

import com.pufferfishscheduler.common.node.Tree;

import java.util.List;

public interface RedisPluginService {

    /**
     * 获取Redis数据源
     *
     * @return
     */
    List<Tree> redisDbTree();
}
