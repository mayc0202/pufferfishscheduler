package com.pufferfishscheduler.master.collect.trans.plugin.controller;

import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.collect.FieldStreamForm;
import com.pufferfishscheduler.master.collect.trans.plugin.service.TableOutputService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;

/**
 * 表输出插件控制器
 * 
 * @author mayc
 */
@Tag(name = OpenApiTags.TRANS_FLOW_COMPONENT, description = OpenApiTags.TRANS_FLOW_COMPONENT_DESC)
@Validated
@RestController
@RequestMapping("/plugin/tableOutput")
public class TableOutputController {

    @Autowired
    private TableOutputService tableOutputService;

    /**
     * 获取转换流字段流
     *
     * @param form 字段流表单
     * @return 字段流
     */
    @Operation(summary = "获取转换流字段流")
    @PostMapping(value = "/getFieldStream.do")
    public ApiResponse getFieldStream(@RequestBody @Valid FieldStreamForm form) {
        return ApiResponse.success(tableOutputService.getFieldStream(form));
    }
}
