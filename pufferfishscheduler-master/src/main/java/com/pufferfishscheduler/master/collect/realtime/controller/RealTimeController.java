package com.pufferfishscheduler.master.collect.realtime.controller;

import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.collect.RealTimeTaskForm;
import com.pufferfishscheduler.master.collect.realtime.service.RealTimeTaskService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * 实时数据采集接口
 */
@Tag(name = OpenApiTags.REALTIME_TASK, description = OpenApiTags.REALTIME_TASK_DESC)
@RestController
@RequestMapping("/realtime/task")
public class RealTimeController {

    @Autowired
    private RealTimeTaskService realTimeTaskService;

    /**
     * 查询实时数据同步任务列表
     * 
     * @param taskName   任务名称
     * @param sourceDbId 源数据库ID
     * @param targetDbId 目标数据库ID
     * @param taskStatus 任务状态
     * @param pageNo     页码
     * @param pageSize   每页数量
     * @return 实时数据同步任务VO列表
     */
    @Operation(summary = "查询实时数据同步任务列表")
    @GetMapping("/list.do")
    public ApiResponse list(
            @RequestParam(required = false) String taskName,
            @RequestParam(required = false) Integer sourceDbId,
            @RequestParam(required = false) Integer targetDbId,
            @RequestParam(required = false) String taskStatus,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse
                .success(realTimeTaskService.list(taskName, sourceDbId, targetDbId, taskStatus, pageNo, pageSize));
    }

    /**
     * 创建实时数据同步任务
     * 
     * @param taskForm 实时数据同步任务表单
     * @return 实时数据同步任务VO
     */
    @Operation(summary = "创建实时数据同步任务")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody RealTimeTaskForm taskForm) {
        realTimeTaskService.add(taskForm);
        return ApiResponse.success("实时数据同步任务任务添加成功！");
    }

    /**
     * 更新实时数据同步任务（仅未启动/已停止/失败/异常状态可修改）
     *
     * @param taskForm 任务表单（含 taskId）
     * @return 操作结果
     */
    @Operation(summary = "更新实时数据同步任务")
    @PostMapping("/update.do")
    public ApiResponse update(@RequestBody RealTimeTaskForm taskForm) {
        realTimeTaskService.update(taskForm);
        return ApiResponse.success("实时数据同步任务更新成功！");
    }

    /**
     * 删除实时数据同步任务（逻辑删除，仅未启动/已停止/失败/异常状态可删）
     *
     * @param taskId 任务ID
     * @return 操作结果
     */
    @Operation(summary = "删除实时数据同步任务")
    @PostMapping("/delete.do")
    public ApiResponse delete(@RequestParam Integer taskId) {
        realTimeTaskService.delete(taskId);
        return ApiResponse.success("实时数据同步任务删除成功！");
    }

    /**
     * 查询实时数据同步任务详情（含表映射、字段映射）
     *
     * @param taskId 任务ID
     * @return 任务详情VO
     */
    @Operation(summary = "查询实时数据同步任务详情")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam Integer taskId) {
        return ApiResponse.success(realTimeTaskService.detail(taskId));
    }

    /**
     * 启动实时数据同步任务（仅未启动/已停止/失败/异常状态可启动）
     *
     * @param taskId 任务ID
     * @return 操作结果
     */
    @Operation(summary = "启动实时数据同步任务")
    @PostMapping("/start.do")
    public ApiResponse start(@RequestParam Integer taskId) {
        realTimeTaskService.immediatelyStart(taskId);
        return ApiResponse.success("实时数据同步任务启动成功！");
    }

    /**
     * 停止实时数据同步任务（仅运行中状态可停止）
     *
     * @param taskId 任务ID
     * @return 操作结果
     */
    @Operation(summary = "停止实时数据同步任务")
    @PostMapping("/stop.do")
    public ApiResponse stop(@RequestParam Integer taskId) {
        realTimeTaskService.immediatelyStop(taskId);
        return ApiResponse.success("实时数据同步任务停止成功！");
    }
}
