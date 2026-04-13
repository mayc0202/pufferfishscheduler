package com.pufferfishscheduler.master.collect.trans.plugin.service.impl;

import com.pufferfishscheduler.common.enums.DatabaseCategory;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.master.collect.trans.plugin.service.RedisPluginService;
import com.pufferfishscheduler.master.database.database.service.DbGroupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Redis插件服务实现类
 */
@Service
public class RedisPluginServiceImpl implements RedisPluginService {

    @Autowired
    private DbGroupService dbGroupService;

    /**
     * 获取FTP数据源
     *
     * @return
     */
    @Override
    public List<Tree> redisDbTree() {
        return dbGroupService.dbTreeByCategory(DatabaseCategory.NO_SQL);
    }
}
