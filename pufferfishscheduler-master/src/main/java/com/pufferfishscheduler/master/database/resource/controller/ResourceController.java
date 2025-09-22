package com.pufferfishscheduler.master.database.resource.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.database.ResourceForm;
import com.pufferfishscheduler.service.database.resource.service.ResourceService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Api(tags = "资源管理")
@Validated
@RestController
@RequestMapping(value = "/resource", produces = {"application/json;charset=utf-8"})
public class ResourceController {

    @Autowired
    private ResourceService service;

    /**
     * 获取资源分组
     *
     * @param name
     * @return
     */
    @ApiOperation(value = "获取资源分组")
    @GetMapping("/tree.do")
    public ApiResponse tree(@RequestParam(required = false) String name) {
        return ApiResponse.success(service.tree(name));
    }

    /**
     * 文件列表
     *
     * @param dbId
     * @param name
     * @param path
     * @param pageNo
     * @param pageSize
     * @return
     */
    @ApiOperation(value = "文件列表")
    @GetMapping("/list.do")
    public ApiResponse list(@RequestParam(required = false) Integer dbId,
                        @RequestParam(required = false) String name,
                        @RequestParam(required = false) String path,
                        @RequestParam(defaultValue = "1") Integer pageNo,
                        @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse.success(service.list(dbId, name, path, pageNo, pageSize));
    }

    /**
     * 上传文件
     *
     * @param dbId
     * @param path
     * @param files
     * @return
     */
    
    @ApiOperation(value = "上传文件")
    @PostMapping(value = "/upload.do")
    public ApiResponse upload(@RequestParam Integer dbId,
                          @RequestParam String path,
                          @RequestPart("files") List<MultipartFile> files) {
        service.upload(dbId, path, files);
        return ApiResponse.success("文件上传成功!");
    }

    /**
     * 创建目录
     *
     * @param form
     * @return
     */
    @ApiOperation(value = "创建目录")
    @PostMapping("/mkdir.do")
    public ApiResponse mkdir(@RequestBody ResourceForm form) {
        service.mkdir(form);
        return ApiResponse.success(String.format("目录%s创建成功!", form.getName()));
    }

    /**
     * 目录重命名
     *
     * @param form
     * @return
     */
    @ApiOperation(value = "目录重命名")
    @PutMapping("/rename.do")
    public ApiResponse rename(@RequestBody ResourceForm form) {
        service.rename(form);
        return ApiResponse.success(String.format("资源重命名%s成功!", form.getNewName()));
    }

    /**
     * 移动资源
     *
     * @param form
     * @return
     */
    @ApiOperation(value = "移动资源")
    @PostMapping("/move.do")
    public ApiResponse move(@RequestBody ResourceForm form) {
        service.move(form);
        return ApiResponse.success("资源移动成功!");
    }

    /**
     * 删除资源
     *
     * @param dbId
     * @param type
     * @param path
     * @return
     */
    @ApiOperation(value = "删除资源")
    @DeleteMapping("/remove.do")
    public ApiResponse remove(@RequestParam Integer dbId,
                          @RequestParam String type,
                          @RequestParam String path) {
        service.remove(dbId, type, path);
        return ApiResponse.success("资源删除成功!");
    }

    /**
     * 获取资源目录列表
     *
     * @param dbId
     * @param path
     * @return
     */
    @ApiOperation(value = "获取资源目录列表(树形结构)")
    @GetMapping("/directoryTree.do")
    public ApiResponse directoryTree(@RequestParam Integer dbId, @RequestParam String path) {
        return ApiResponse.success(service.directoryTree(dbId, path));
    }
}
