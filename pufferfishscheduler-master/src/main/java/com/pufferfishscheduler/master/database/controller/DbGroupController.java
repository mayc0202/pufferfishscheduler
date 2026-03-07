package com.pufferfishscheduler.master.database.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.database.DbGroupForm;
import com.pufferfishscheduler.master.database.service.DbGroupService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;


/**
 * 数据源分组控制层
 *
 * @author mayc
 * @since 2025-05-22 00:00:40
 */
@Tag(name = "数据源分组管理")
@Validated
@RestController
@RequestMapping(value = "/dbGroup", produces = {"application/json;charset=utf-8"})
public class DbGroupController {

    @Autowired
    private DbGroupService dbGroupService;

    /**
     * 获取数据源分组树形结构
     * @param name
     * @return
     */
    @Operation(summary = "获取数据源分组")
    @GetMapping("/tree.do")
    public ApiResponse tree(@RequestParam(required = false) String name) {
        return ApiResponse.success(dbGroupService.tree(name));
    }

    /**
     * 获取FTP数据源树形结构
     *
     * @param name
     * @return
     */
    @Operation(summary = "获取FTP数据源树形结构")
    @GetMapping("/ftpDbTree.do")
    public ApiResponse ftpDbTree(@RequestParam(required = false) String name) {
        return ApiResponse.success(dbGroupService.ftpDbTree(name));
    }

    /**
     * 获取关系型数据源树形结构
     *
     * @return
     */
    @Operation(summary = "获取关系型数据源树形结构")
    @GetMapping("/relationalDbTree.do")
    public ApiResponse relationalDbTree() {
        return ApiResponse.success(dbGroupService.relationalDbTree());
    }

    /**
     * 添加数据源分组
     *
     * @param form
     * @return
     */
    @Operation(summary = "添加数据源分组")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid DbGroupForm form) {
        dbGroupService.add(form);
        return ApiResponse.success("分组添加成功!");
    }

    /**
     * 编辑数据源分组
     *
     * @param form
     * @return
     */
    @Operation(summary = "编辑数据源分组")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid DbGroupForm form) {
        dbGroupService.update(form);
        return ApiResponse.success("分组修改成功!");
    }

    /**
     * 删除数据源分组
     *
     * @param id
     * @return
     */
    @Operation(summary = "删除数据源分组")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam Integer id) {
        dbGroupService.delete(id);
        return ApiResponse.success("分组删除成功!");
    }

}

