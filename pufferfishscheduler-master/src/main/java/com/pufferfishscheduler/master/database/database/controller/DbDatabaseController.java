package com.pufferfishscheduler.master.database.database.controller;

import com.pufferfishscheduler.domain.form.database.DbDatabaseForm;
import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.master.database.database.service.DbFieldService;
import com.pufferfishscheduler.master.database.database.service.DbTableService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.Objects;

/**
 * 数据源控制层
 *
 * @author mayc
 * @since 2025-06-03 21:22:24
 */
@Tag(name = OpenApiTags.DATASOURCE,description = OpenApiTags.DATASOURCE_DESC)
@Validated
@RestController
@RequestMapping(value = "/dbDatabase", produces = { "application/json;charset=utf-8" })
public class DbDatabaseController {

    @Autowired
    private DbDatabaseService dbDatabaseService;

    @Autowired
    private DbTableService dbTableService;

    @Autowired
    private DbFieldService dbFieldService;

    /**
     * 获取数据源集合
     *
     * @param groupId
     * @param dbId
     * @param name
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Operation(summary = "获取数据源列表")
    @GetMapping("/list.do")
    public ApiResponse list(@RequestParam(required = false) Integer groupId,
            @RequestParam(required = false) Integer dbId,
            @RequestParam(required = false) String name,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse.success(dbDatabaseService.list(groupId, dbId, name, pageNo, pageSize));
    }

    /**
     * 获取关系表列表
     *
     * @param dbId
     * @return
     */
    @Operation(summary = "获取关系表列表")
    @GetMapping("/dbTableList.do")
    public ApiResponse dbTableList(@RequestParam(required = false) Integer dbId) {
        return ApiResponse.success(dbTableService.getTablesByDbId(dbId));
    }

     /**
     * 获取字段列表
     *
     * @param tableId
     * @return
     */
    @Operation(summary = "获取字段列表")
    @GetMapping("/fieldList.do")
    public ApiResponse fieldList(@RequestParam(required = false) Integer tableId) {
        return ApiResponse.success(dbFieldService.getFieldsByTableId(tableId));
    }

    /**
     * 保存数据源配置
     *
     * @return
     */
    @Operation(summary = "保存数据源配置")
    @PostMapping("/save.do")
    public ApiResponse save(@RequestBody @Valid DbDatabaseForm form) {
        if (Objects.isNull(form.getId())) {
            dbDatabaseService.add(form);
            return ApiResponse.success("数据源接入成功!");
        } else {
            dbDatabaseService.update(form);
            return ApiResponse.success("数据源编辑成功!");
        }
    }

    /**
     * 删除数据源
     *
     * @return
     */
    @Operation(summary = "删除数据源")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam Integer id) {
        dbDatabaseService.delete(id);
        return ApiResponse.success("数据源删除成功!");
    }

    /**
     * 获取数据源详情
     *
     * @param id
     * @return
     */
    @Operation(summary = "获取数据源详情")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam Integer id) {
        return ApiResponse.success(dbDatabaseService.detail(id));
    }

    /**
     * 测试连接
     *
     * @param form
     * @return
     */
    @Operation(summary = "测试链接")
    @PostMapping("/connect.do")
    public ApiResponse connect(@RequestBody @Valid DbDatabaseForm form) {
        dbDatabaseService.testConnect(form);
        return ApiResponse.success("连接成功!");
    }
}
