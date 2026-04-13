package com.pufferfishscheduler.master.collect.trans.plugin.controller;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.collect.trans.plugin.service.DebeziumPluginService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Debezium格式JSON转换插件
 */
@Tag(name = OpenApiTags.TRANS_FLOW_COMPONENT, description = OpenApiTags.TRANS_FLOW_COMPONENT_DESC)
@Validated
@RestController
@RequestMapping("/plugin/debezium")
public class DebeziumPluginController {

    @Autowired
    private DebeziumPluginService debeziumPluginService;

    /**
     * 结构报文数据
     *
     * @param sample
     * @return
     */
    @Operation(summary = "结构报文数据")
    @PostMapping("/parseSampleData.do")
    public ApiResponse parseSampleData(@RequestBody JSONObject sample) {
        return ApiResponse.success(debeziumPluginService.parseSampleData(sample));
    }
}
