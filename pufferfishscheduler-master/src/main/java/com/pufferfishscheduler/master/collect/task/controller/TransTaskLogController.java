package com.pufferfishscheduler.master.collect.task.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.vo.collect.TransTaskLogVo;
import com.pufferfishscheduler.master.collect.task.service.TransTaskLogService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

/**
 *
 * @author Mayc
 * @since 2026-03-23  16:03
 */
@Tag(name = OpenApiTags.TRANS_TASK, description = OpenApiTags.TRANS_TASK_DESC)
@Validated
@RestController
@RequestMapping(value = "/trans/task/log", produces = {"application/json;charset=utf-8"})
public class TransTaskLogController {

    @Autowired
    private TransTaskLogService transTaskLogService;

    /**
     * 分页查询转换任务日志
     */
    @Operation(summary = "分页查询转换任务日志")
    @GetMapping("/list.do")
    public ApiResponse list(@RequestParam(required = false) String taskName,
                            @RequestParam(required = false) String startTime,
                            @RequestParam(required = false) String endTime,
                            @RequestParam(required = false) String status,
                            @RequestParam(defaultValue = "1") Integer pageNo,
                            @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse.success(transTaskLogService.list(taskName, startTime, endTime, status, pageNo, pageSize));
    }

    /**
     * 查询转换任务日志详情
     */
    @Operation(summary = "查询转换任务日志详情")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam String id) {
        return ApiResponse.success(transTaskLogService.detail(id));
    }
}
