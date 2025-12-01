package com.pufferfishscheduler.master.controller.database;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.metadata.MetadataTaskForm;
import com.pufferfishscheduler.domain.form.metadata.MetadataTaskUpdateForm;
import com.pufferfishscheduler.service.database.MetadataService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * 元数据控制层
 *
 * @author Mayc
 * @since 2025-11-17  17:18
 */
@Api(tags = "元数据管理")
@Validated
@RestController
@RequestMapping(value = "/metadata", produces = {"application/json;charset=utf-8"})
public class MetaDataController {

    @Autowired
    private MetadataService metaDataService;

    @ApiOperation(value = "获取同步任务分组")
    @GetMapping("/tree.do")
    public ApiResponse tree(@RequestParam(required = false) String name) {
        return ApiResponse.success(metaDataService.tree(name));
    }

    @ApiOperation(value = "获取同步任务列表")
    @GetMapping("/list.do")
    public ApiResponse list(
            @RequestParam(required = false) Integer dbId,
            @RequestParam(required = false) String dbName,
            @RequestParam(required = false) Integer groupId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Boolean enable,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "10") Integer pageSize
    ) {
        return ApiResponse.success(metaDataService.list(dbId, dbName, groupId, status, enable, pageNo, pageSize));
    }

    /**
     * 获取同步任务详情
     *
     * @param id
     * @return
     */
    @ApiOperation(value = "获取同步任务详情")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam Integer id) {
        return ApiResponse.success(metaDataService.detail(id));
    }

    /**
     * 新增元数据同步任务
     *
     * @param taskForm
     * @return
     */
    @ApiOperation(value = "新增元数据同步任务")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody MetadataTaskForm taskForm) {
        metaDataService.add(taskForm);
        return ApiResponse.success("元数据任务新增成功!");
    }

    /**
     * 编辑元数据同步任务
     *
     * @param taskForm
     * @return
     */
    @ApiOperation(value = "编辑元数据同步任务")
    @PostMapping("/update.do")
    public ApiResponse update(@RequestBody MetadataTaskUpdateForm taskForm) {
        metaDataService.update(taskForm);
        return ApiResponse.success("元数据任务编辑成功!");
    }

    /**
     * 切换启用状态
     *
     * @param id
     * @return
     */
    @ApiOperation(value = "切换启用状态")
    @GetMapping("/toggleEnableStatus.do")
    public ApiResponse toggleEnableStatus(@RequestParam Integer id) {
        metaDataService.toggleEnableStatus(id);
        return ApiResponse.success("元数据任务切换启用状态成功");
    }

    /**
     * 删除同步任务
     *
     * @param id
     * @return
     */
    @ApiOperation(value = "删除同步任务")
    @GetMapping("/delete.do")
    public ApiResponse delete(@RequestParam Integer id) {
        metaDataService.delete(id);
        return ApiResponse.success("元数据任务删除成功");
    }

    /**
     * 同步数据
     *
     * @param dbId
     * @return
     */
    @ApiOperation(value = "同步数据")
    @PostMapping("/sync.do")
    public ApiResponse sync(@RequestParam Integer dbId) {
        metaDataService.metadataSync(dbId);
        return ApiResponse.success("开始同步!");
    }

}
