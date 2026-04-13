package com.pufferfishscheduler.master.dashboard.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import com.pufferfishscheduler.master.dashboard.service.DashboardService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 首页仪表盘统计接口
 */
@Tag(name = OpenApiTags.DASHBOARD, description = OpenApiTags.DASHBOARD_DESC)
@RestController
@RequestMapping(value = "/dashboard", produces = {"application/json;charset=utf-8"})
public class DashboardController {

    @Autowired
    private DashboardService dashboardService;

    @Operation(summary = "首页仪表盘汇总统计")
    @GetMapping("/summary.do")
    public ApiResponse summary() {
        return ApiResponse.success(dashboardService.summary());
    }
}

