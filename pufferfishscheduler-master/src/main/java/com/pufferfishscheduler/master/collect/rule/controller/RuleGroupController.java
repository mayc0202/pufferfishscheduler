package com.pufferfishscheduler.master.collect.rule.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.collect.RuleGroupForm;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import com.pufferfishscheduler.master.collect.rule.service.RuleGroupService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * 规则组接口
 *
 * @author Mayc
 * @since 2026-03-18  15:30
 */
@Tag(name = OpenApiTags.RULE,description = OpenApiTags.RULE_DESC)
@Validated
@RestController
@RequestMapping(value = "/rule/group", produces = {"application/json;charset=utf-8"})
public class RuleGroupController {

    @Autowired
    private RuleGroupService ruleGroupService;

    @Operation(summary = "获取规则分类树")
    @GetMapping("/tree.do")
    public ApiResponse tree(@RequestParam(required = false) String name) {
        return ApiResponse.success(ruleGroupService.tree(name));
    }

    @Operation(summary = "新增规则分类")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid RuleGroupForm form) {
        ruleGroupService.add(form);
        return ApiResponse.success("规则分类新增成功!");
    }

    @Operation(summary = "编辑规则分类")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid RuleGroupForm form) {
        ruleGroupService.update(form);
        return ApiResponse.success("规则分类编辑成功!");
    }

    @Operation(summary = "删除规则分类")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam Integer id) {
        ruleGroupService.delete(id);
        return ApiResponse.success("规则分类删除成功!");
    }

    @Operation(summary = "获取固定分类ID")
    @GetMapping("/regularGroupId.do")
    public ApiResponse regularGroupId() {
        return ApiResponse.success(ruleGroupService.getRegularGroupId());
    }
}
