package com.pufferfishscheduler.master.collect.trans.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.collect.TransFlowConfigForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowForm;
import com.pufferfishscheduler.master.collect.trans.service.TransFlowService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * 转换流控制层
 *
 * @author Mayc
 * @since 2026-03-04 17:44
 */
@Tag(name = "转换流程管理", description = "")
@Validated
@RestController
@RequestMapping(value = "/trans/flow", produces = { "application/json;charset=utf-8" })
public class TransFlowController {

    @Autowired
    private TransFlowService flowService;

    /**
     * 查询转换流程列表
     *
     * @param groupId
     * @param flowName
     * @param pageNo
     * @param pageSize
     * @return 转换流程列表
     */
    @Operation(summary = "查询转换流程列表")
    @GetMapping("/list.do")
    public ApiResponse list(@RequestParam(required = false, name = "groupId") Integer groupId,
            @RequestParam(required = false, name = "flowName") String flowName,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse.success(flowService.list(groupId, flowName, pageNo, pageSize));
    }

    /**
     * 添加转换流程
     *
     * @param form 转换流程表单
     * @return 转换流程id
     */
    @Operation(summary = "添加转换流程")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid TransFlowForm form) {
        flowService.addFlow(form);
        return ApiResponse.success("流程添加成功!");
    }

    /**
     * 保存转换流程配置
     *
     * @param form 转换流程表单
     * @return 转换流程id
     */
    @Operation(summary = "保存转换流程配置")
    @PostMapping("/setConfig.do")
    public ApiResponse setConfig(@RequestBody @Valid TransFlowConfigForm form) {
        flowService.setConfig(form);
        return ApiResponse.success("流程配置保存成功!");
    }

    /**
     * 执行转换流
     * 
     * @param id
     * @return
     */
    @GetMapping(value = "/execute.do")
    public ApiResponse execute(@RequestParam Integer id) {
        flowService.execute(id);
        return ApiResponse.success("转换流程启动成功！");
    }

    /**
     * 保存转换流程配置
     *
     * @param id 流程id
     * @return 转换流程id
     */
    @Operation(summary = "保存转换流程配置")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam(name = "id") Integer id) {
        return ApiResponse.success(flowService.detail(id));
    }

    /**
     * 编辑转换流程
     *
     * @param form 转换流程表单
     * @return 转换流程id
     */
    @Operation(summary = "编辑转换流程")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid TransFlowForm form) {
        flowService.updateFlow(form);
        return ApiResponse.success("流程修改成功!");
    }

    /**
     * 删除转换流程
     *
     * @param id 转换流程id
     * @return 转换流程id
     */
    @Operation(summary = "删除转换流程")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam(name = "") Integer id) {
        flowService.deleteFlow(id);
        return ApiResponse.success("流程删除成功!");
    }

    /**
     * 预览数据
     *
     * @param id
     * @return
     */
    @GetMapping(value = "/preview.do")
    public ApiResponse preview(@RequestParam Integer id, @RequestParam String stepName) {
        return ApiResponse.success(flowService.preview(id, stepName));
    }
}
