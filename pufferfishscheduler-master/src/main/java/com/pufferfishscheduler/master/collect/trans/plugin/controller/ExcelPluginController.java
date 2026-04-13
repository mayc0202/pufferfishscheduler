package com.pufferfishscheduler.master.collect.trans.plugin.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.collect.TransConfigForm;
import com.pufferfishscheduler.master.collect.trans.plugin.service.ExcelPluginService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@Tag(name = OpenApiTags.TRANS_FLOW_COMPONENT, description = OpenApiTags.TRANS_FLOW_COMPONENT_DESC)
@RestController
@RequestMapping("/plugin/excel")
public class ExcelPluginController {

    @Autowired
    private ExcelPluginService excelPluginService;

    /**
     * 获取所有的文件
     *
     * @param dbId 数据库ID
     * @param path 路径
     * @return {@link ApiResponse}<{@link List}<{@link ResourceVo>}>
     */
    @Operation(summary = "获取FTP目录下所有的xlsx、xls文件")
    @GetMapping("/getResources.do")
    public ApiResponse getResources(Integer dbId, String path) {
        return ApiResponse.success(excelPluginService.getResources(dbId, path));
    }

    /**
     * 获取所有的sheet名称
     *
     * @param form 配置表单
     * @return {@link ApiResponse}<{@link Set}<{@link String>}>
     */
    @Operation(summary = "获取所有的sheet名称")
    @PostMapping("/getSheets.do")
    public ApiResponse getSheets(@RequestBody TransConfigForm form) {
        return ApiResponse.success(excelPluginService.getSheets(form));
    }

    /**
     * 获取所有的字段
     *
     * @param form 配置表单
     * @return {@link ApiResponse}<{@link List}<{@link BaseFieldVo>}>
     */
    @Operation(summary = "获取所有的字段")
    @PostMapping("/getFields.do")
    public ApiResponse getFields(@RequestBody TransConfigForm form) {
        return ApiResponse.success(excelPluginService.getFields(form));
    }
}
