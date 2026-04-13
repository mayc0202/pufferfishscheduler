package com.pufferfishscheduler.master.dashboard.service;

import com.pufferfishscheduler.domain.vo.dashboard.DashboardSummaryVo;

/**
 * 首页仪表盘统计服务
 */
public interface DashboardService {

    /**
     * 首页汇总统计（一次性返回首页所需数据）
     */
    DashboardSummaryVo summary();
}

