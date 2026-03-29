package com.pufferfishscheduler.master.collect.rule.controller;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.common.utils.ExcelUtil;
import com.pufferfishscheduler.domain.form.collect.RuleForm;
import com.pufferfishscheduler.master.collect.rule.service.RuleService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

/**
 * 规则管理接口
 */
@Tag(name = OpenApiTags.RULE, description = OpenApiTags.RULE_DESC)
@Validated
@RestController
@RequestMapping(value = "/rule", produces = {"application/json;charset=utf-8"})
public class RuleController {

    @Autowired
    private RuleService ruleService;

    /**
     * 分页查询规则
     * @param groupId    分组ID
     * @param ruleName   规则名称
     * @param pageNo     页码
     * @param pageSize   每页数量
     * @return 规则列表
     */
    @Operation(summary = "分页查询规则")
    @GetMapping("/list.do")
    public ApiResponse list(
            @RequestParam(required = false) Integer groupId,
            @RequestParam(required = false) String ruleName,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse.success(ruleService.list(groupId, ruleName, pageNo, pageSize));
    }

    /**
     * 规则详情
     * @param id 规则ID
     * @return 规则详情
     */
    @Operation(summary = "规则详情")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam String id) {
        return ApiResponse.success(ruleService.detail(id));
    }

    /**
     * 新增规则
     * @param form 规则表单
     */
    @Operation(summary = "新增规则")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid RuleForm form) {
        ruleService.add(form);
        return ApiResponse.success("规则新增成功!");
    }

    /**
     * 编辑规则
     * @param form 规则表单
     */
    @Operation(summary = "编辑规则")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid RuleForm form) {
        ruleService.update(form);
        return ApiResponse.success("规则编辑成功!");
    }

    /**
     * 逻辑删除规则
     * @param id 规则ID
     */
    @Operation(summary = "删除规则")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam String id) {
        ruleService.delete(id);
        return ApiResponse.success("规则删除成功!");
    }

    /**
     * 发布/禁用规则
     */
    @Operation(summary = "发布/禁用规则")
    @PutMapping("/release.do")
    public ApiResponse release(@RequestParam String id, @RequestParam Boolean status) {
        ruleService.release(id, status);
        return ApiResponse.success("规则状态更新成功!");
    }

    /**
     * 分组+规则树（只包含已发布规则）
     */
    @Operation(summary = "分组+规则树")
    @GetMapping("/tree.do")
    public ApiResponse tree() {
        return ApiResponse.success(ruleService.tree());
    }


    /**
     * 模板下载
     *
     * @return
     */
    @Operation(summary = "模板下载")
    @PostMapping(value = "/downloadTemplate.do", produces = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    public void downloadTemplate(HttpServletResponse response) {
        ExcelUtil.downloadTemplate(response, Constants.TEMPLATE.VALUE_MAPPING);
    }

    /**
     * 模板导入
     *
     * @return
     */
    @Operation(summary = "模板导入")
    @PostMapping(value = "/importTemplate.do", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ApiResponse importTemplate(@RequestParam("file") MultipartFile file) {
        return ApiResponse.success(ExcelUtil.importValueMappingTemplate(file));
    }

    /**
     * 校验Java自定义规则脚本语法
     *
     * @param json
     * @return
     * @throws Exception
     */
    @Operation(summary = "校验Java自定义规则脚本语法")
    @PostMapping("/validateJavaCode.do")
    public ApiResponse validateJavaCode(@RequestBody JSONObject json) {
        ruleService.validateJavaCode(json);
        return ApiResponse.success("java自定义规则脚本语法校验通过!");
    }

    /**
     * 预览Java自定义规则结果
     * @param json
     * @return
     */
    @Operation(summary = "预览Java自定义规则结果")
    @PostMapping("/previewJavaCode.do")
    public ApiResponse previewJavaCode(@RequestBody JSONObject json) {
        return ApiResponse.success(ruleService.previewJavaCode(json));
    }

    /**
     * 校验sql脚本
     *
     * @param json
     * @return
     */
    @Operation(summary = "校验sql脚本")
    @PostMapping("/validateSql.do")
    public ApiResponse validateSql(@RequestBody JSONObject json) {
        ruleService.validateSql(json);
        return ApiResponse.success("sql校验通过");
    }

    /**
     * 预览数据
     *
     * @param json
     * @return
     */
    @Operation(summary = "预览数据")
    @PostMapping("/preview.do")
    public ApiResponse preview(@RequestBody JSONObject json) {
        return ApiResponse.success(ruleService.preview(json));
    }
}
