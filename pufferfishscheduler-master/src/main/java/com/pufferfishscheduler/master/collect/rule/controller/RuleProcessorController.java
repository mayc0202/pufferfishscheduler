package com.pufferfishscheduler.master.collect.rule.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.collect.rule.service.RuleProcessorService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 规则处理器接口（内置，只读）
 *
 * @author Mayc
 * @since 2026-03-18  15:30
 */
@Tag(name = OpenApiTags.RULE, description = OpenApiTags.RULE_DESC)
@RestController
@RequestMapping(value = "/rule/processor", produces = {"application/json;charset=utf-8"})
public class RuleProcessorController {

    @Autowired
    private RuleProcessorService ruleProcessorService;

    @Operation(summary = "获取规则处理器列表")
    @GetMapping("/list.do")
    public ApiResponse list() {
        return ApiResponse.success(ruleProcessorService.list());
    }

    @Operation(summary = "获取规则处理器字典")
    @GetMapping("/dict.do")
    public ApiResponse dict() {
        return ApiResponse.success(ruleProcessorService.dict());
    }

    @Operation(summary = "获取规则处理器详情")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam Integer id) {
        return ApiResponse.success(ruleProcessorService.detail(id));
    }
}
