package com.pufferfishscheduler.master.database.db.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.database.DbGroupForm;
import com.pufferfishscheduler.service.database.db.service.DbGroupService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

/**
 * (DbGroup) Controller
 *
 * @author mayc
 * @since 2025-05-22 00:00:40
 */
@Api(tags = "数据源分组管理")
@Validated
@RestController
@RequestMapping(value = "/dbGroup", produces = {"application/json;charset=utf-8"})
public class DbGroupController {

    @Autowired
    private DbGroupService dbGroupService;
    
    @ApiOperation(value = "获取数据源分组")
    @GetMapping("/tree.do")
    public ApiResponse tree(@RequestParam(required = false) String name) {
        return ApiResponse.success(dbGroupService.tree(name));
    }

    /**
     * Add group
     *
     * @return
     */
  
    @ApiOperation(value = "添加数据源分组")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid DbGroupForm form) {
        dbGroupService.add(form);
        return ApiResponse.success("分组添加成功!");
    }

    /**
     * Edit group
     *
     * @return
     */
  
    @ApiOperation(value = "编辑数据源分组")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid DbGroupForm form) {
        dbGroupService.update(form);
        return ApiResponse.success("分组修改成功!");
    }

    /**
     * Delete group
     *
     * @return
     */
  
    @ApiOperation(value = "删除数据源分组")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam Integer id) {
        dbGroupService.delete(id);
        return ApiResponse.success("分组删除成功!");
    }

}

