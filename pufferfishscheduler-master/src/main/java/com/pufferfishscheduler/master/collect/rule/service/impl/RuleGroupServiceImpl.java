package com.pufferfishscheduler.master.collect.rule.service.impl;

import com.pufferfishscheduler.dao.mapper.RuleGroupMapper;
import com.pufferfishscheduler.master.collect.rule.service.RuleGroupService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 规则组服务实现类
 *
 * @author Mayc
 * @since 2026-03-18  15:59
 */
@Slf4j
@Service
public class RuleGroupServiceImpl implements RuleGroupService {

    @Autowired
    private RuleGroupMapper groupMapper;


}
