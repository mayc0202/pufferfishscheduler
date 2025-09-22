package com.pufferfishscheduler.master.database.db.controller;

import com.pufferfishscheduler.domain.form.database.DbDatabaseForm;
import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.service.database.db.service.DbDatabaseService;
import com.pufferfishscheduler.service.database.db.sync.DbSyncExecutor;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.Objects;

/**
 * (DbDatabase)表控制层
 *
 * @author mayc
 * @since 2025-06-03 21:22:24
 */
@Api(tags = "数据源管理")
@Validated
@RestController
@RequestMapping(value = "/dbDatabase", produces = {"application/json;charset=utf-8"})
public class DbDatabaseController {

    @Autowired
    private DbDatabaseService dbDatabaseService;

    @Autowired
    private DbSyncExecutor dbSyncExecutor;

    /**
     * Get database list
     *
     * @param groupId
     * @param dbId
     * @param name
     * @param pageNo
     * @param pageSize
     * @return
     */
    @ApiOperation(value = "获取数据源列表")
    @GetMapping("/list.do")
    public ApiResponse list(@RequestParam(required = false) Integer groupId,
                            @RequestParam(required = false) Integer dbId,
                            @RequestParam(required = false) String name,
                            @RequestParam(defaultValue = "1") Integer pageNo,
                            @RequestParam(defaultValue = "10") Integer pageSize
    ) {
        return ApiResponse.success(dbDatabaseService.list(groupId, dbId, name, pageNo, pageSize));
    }

    /**
     * 保存数据源配置
     *
     * @return
     */
    @ApiOperation(value = "保存数据源配置")
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
    @ApiOperation(value = "删除数据源")
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
    @ApiOperation(value = "获取数据源详情")
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
    @ApiOperation(value = "测试链接")
    @PostMapping("/connect.do")
    public ApiResponse connect(@RequestBody @Valid DbDatabaseForm form) {
        dbDatabaseService.testConnect(form);
        return ApiResponse.success("连接成功!");
    }

    /**
     * 同步数据
     *
     * @param dbId
     * @return
     */
    @ApiOperation(value = "同步数据")
    @PostMapping("sync.do")
    public ApiResponse sync(@RequestParam Integer dbId) {
        dbSyncExecutor.execute(dbId);
        return ApiResponse.success("开始同步!");
    }
}

