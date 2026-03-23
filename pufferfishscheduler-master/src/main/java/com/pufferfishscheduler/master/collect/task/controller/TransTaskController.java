package com.pufferfishscheduler.master.collect.task.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.collect.TransTaskForm;
import com.pufferfishscheduler.master.collect.task.service.TransTaskService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * 转换任务控制层（风格对齐元数据任务接口）
 *
 * @author Mayc
 * @since 2026-03-20
 */
@Tag(name = OpenApiTags.TRANS_TASK, description = OpenApiTags.TRANS_TASK_DESC)
@Validated
@RestController
@RequestMapping(value = "/trans/task", produces = { "application/json;charset=utf-8" })
public class TransTaskController {

    @Autowired
    private TransTaskService transTaskService;

    @Operation(summary = "分页查询转换任务")
    @GetMapping("/list.do")
    public ApiResponse list(
            @RequestParam(required = false) Integer groupId,
            @RequestParam(required = false) String name,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Boolean enable,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse.success(transTaskService.list(groupId, name, status, enable, pageNo, pageSize));
    }

    @Operation(summary = "转换任务详情")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam Integer id) {
        return ApiResponse.success(transTaskService.detail(id));
    }

    @Operation(summary = "新增转换任务")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid TransTaskForm taskForm) {
        transTaskService.add(taskForm);
        return ApiResponse.success("转换任务新增成功!");
    }

    @Operation(summary = "编辑转换任务")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid TransTaskForm taskForm) {
        transTaskService.update(taskForm);
        return ApiResponse.success("转换任务编辑成功!");
    }

    @Operation(summary = "删除转换任务")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam Integer id) {
        transTaskService.delete(id);
        return ApiResponse.success("转换任务删除成功!");
    }

    @Operation(summary = "启用转换任务")
    @PutMapping("/enable.do")
    public ApiResponse enable(@RequestParam Integer id) {
        transTaskService.enable(id);
        return ApiResponse.success("转换任务已启用!");
    }

    @Operation(summary = "停用转换任务")
    @PutMapping("/disable.do")
    public ApiResponse disable(@RequestParam Integer id) {
        transTaskService.disable(id);
        return ApiResponse.success("转换任务已停用!");
    }

    @Operation(summary = "立即执行转换任务")
    @PostMapping("/immediatelyExecute.do")
    public ApiResponse immediatelyExecute(@RequestParam Integer id) {
        transTaskService.immediatelyExecute(id);
        return ApiResponse.success("已受理，转换任务执行中");
    }

    @Operation(summary = "立即停止转换任务")
    @PostMapping("/immediatelyStop.do")
    public ApiResponse immediatelyStop(@RequestParam Integer id) {
        transTaskService.immediatelyStop(id);
        return ApiResponse.success("已受理，转换任务停止中!");
    }
}
