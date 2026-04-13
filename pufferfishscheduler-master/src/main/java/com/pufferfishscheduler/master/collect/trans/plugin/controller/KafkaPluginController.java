package com.pufferfishscheduler.master.collect.trans.plugin.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.collect.trans.plugin.service.KafkaPluginService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Kafka 插件控制器
 *
 * @author mayc
 */
@Tag(name = OpenApiTags.TRANS_FLOW_COMPONENT, description = OpenApiTags.TRANS_FLOW_COMPONENT_DESC)
@Validated
@RestController
@RequestMapping("/plugin/kafka")
public class KafkaPluginController {

    @Autowired
    private KafkaPluginService kafkaPluginService;

    /**
     * 获取MQ数据源树形结构
     *
     * @return
     */
    @Operation(summary = "获取MQ数据源树形结构")
    @GetMapping("/mqDbTree.do")
    public ApiResponse mqDbTree() {
        return ApiResponse.success(kafkaPluginService.mqDbTree());
    }

    @Operation(summary = "获取topic列表")
    @GetMapping("/topics.do")
    public ApiResponse topics(@RequestParam Integer id) {
        return ApiResponse.success(kafkaPluginService.topics(id));
    }
}
