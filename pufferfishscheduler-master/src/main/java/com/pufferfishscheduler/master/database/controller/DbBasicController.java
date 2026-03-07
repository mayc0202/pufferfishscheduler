package com.pufferfishscheduler.master.database.controller;


import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.database.service.DbBasicService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


/**
 * 数据库基础信息控制层
 *
 * @author mayc
 * @since 2025-05-20 23:34:42
 */
@Tag(name = "数据源信息管理")
@RestController
@RequestMapping(value = "/dbBasic", produces = {"application/json;charset=utf-8"})
public class DbBasicController {

    @Autowired
    private DbBasicService dbBasicService;

    /**
     * 获取数据库类型集合
     *
     * @return
     */
    @Operation(summary = "获取数据库类型集合")
    @GetMapping("/getDbCategoryList.do")
    public ApiResponse getDbCategoryList() {
        return ApiResponse.success(dbBasicService.getDbCategoryList());
    }

    /**
     * 获取数据库基础信息集合
     *
     * @return
     */
    @Operation(summary = "获取数据库基础信息集合")
    @GetMapping("/getDbBasicList.do")
    public ApiResponse getDbBasicList() {
        return ApiResponse.success(dbBasicService.getDbBasicList());
    }

    /**
     * 根据数据库类别id获取数据库基础信息集合
     *
     * @param id
     * @return
     */
    @Operation(summary = "根据数据库类别id获取数据库基础信息集合")
    @GetMapping("/getDbBasicListByCategoryId.do")
    public ApiResponse getDbBasicListByCategoryId(@RequestParam Integer id) {
        return ApiResponse.success(dbBasicService.getDbBasicListByCategoryId(id));
    }

}

