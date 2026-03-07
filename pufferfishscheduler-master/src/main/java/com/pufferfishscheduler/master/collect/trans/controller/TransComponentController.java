package com.pufferfishscheduler.master.collect.trans.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.collect.trans.service.TransComponentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 转换组件控制器
 *
 * @author Mayc
 * @since 2026-03-02  23:38
 * @since 2026-03-02  22:49
 */
@Tag(name = "转换组件管理")
@RestController
@RequestMapping(value = "/trans/component", produces = {"application/json;charset=utf-8"})

public class TransComponentController {

    @Autowired
    private TransComponentService transComponentService;

    /**
     * 获取转换组件树结构
     *
     * @return
     */
    @Operation(summary = "获取转换组件树结构")
    @GetMapping("/tree.do")
    public ApiResponse tree() {
        return ApiResponse.success(transComponentService.getComponentTree());
    }
}
