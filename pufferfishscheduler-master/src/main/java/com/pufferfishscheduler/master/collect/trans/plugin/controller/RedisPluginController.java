package com.pufferfishscheduler.master.collect.trans.plugin.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.collect.trans.plugin.service.RedisPluginService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = OpenApiTags.TRANS_FLOW_COMPONENT, description = OpenApiTags.TRANS_FLOW_COMPONENT_DESC)
@Validated
@RestController
@RequestMapping("/plugin/redis")
public class RedisPluginController {

    @Autowired
    private RedisPluginService redisPluginService;

    /**
     * 获取Redis数据源树形结构
     *
     * @return
     */
    @Operation(summary = "获取Redis数据源树形结构")
    @GetMapping("/redisDbTree.do")
    public ApiResponse redisDbTree() {
        return ApiResponse.success(redisPluginService.redisDbTree());
    }
}
