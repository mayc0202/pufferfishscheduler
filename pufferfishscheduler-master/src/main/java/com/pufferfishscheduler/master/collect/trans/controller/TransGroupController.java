package com.pufferfishscheduler.master.collect.trans.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.collect.TransGroupForm;
import com.pufferfishscheduler.master.collect.trans.service.TransGroupService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * 转换分组控制层
 *
 * @author mayc
 * @since 2026-03-04
 */
@Tag(name = "转换分组管理",description = "")
@Validated
@RestController
@RequestMapping(value = "/trans/group", produces = {"application/json;charset=utf-8"})
public class TransGroupController {

    @Autowired
    private TransGroupService transGroupService;

    /**
     * 获取转换分组树形结构
     * @param name 分组名称
     * @return 转换分组树形结构
     */
    @Operation(summary  = "获取转换分组树形结构", description = "")
    @GetMapping("/tree.do")
    public ApiResponse tree(@RequestParam(required = false) String name) {
        return ApiResponse.success(transGroupService.tree(name));
    }

    /**
     * 添加转换分组
     *
     * @return
     */
    @Operation(summary = "添加转换分组")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid TransGroupForm form) {
        transGroupService.add(form);
        return ApiResponse.success("分组添加成功!");
    }

    /**
     * 编辑转换分组
     *
     * @return
     */
    @Operation(summary = "编辑转换分组")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid TransGroupForm form) {
        transGroupService.update(form);
        return ApiResponse.success("分组修改成功!");
    }

    /**
     * 删除转换分组
     *
     * @return
     */
    @Operation(summary = "删除转换分组")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam Integer id) {
        transGroupService.delete(id);
        return ApiResponse.success("分组删除成功!");
    }

}
