package com.pufferfishscheduler.master.resource.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.database.ResourceForm;
import com.pufferfishscheduler.master.resource.service.ResourceService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

/**
 * 资源控制器
 *
 * @author mayc
 * @since 2026-03-04
 */
@Tag(name = "资源管理")
@Validated
@RestController
@RequestMapping(value = "/resource", produces = {"application/json;charset=utf-8"})
public class ResourceController {

    @Autowired
    private ResourceService service;

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
    @Operation(summary = "获取资源列表")
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
    @Operation(summary = "上传文件")
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
    @Operation(summary = "创建目录")
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
    @Operation(summary = "目录重命名")
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
    @Operation(summary = "移动资源")
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
    @Operation(summary = "删除资源")
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
    @Operation(summary = "获取资源目录列表")
    @GetMapping("/directoryTree.do")
    public ApiResponse directoryTree(@RequestParam Integer dbId, @RequestParam String path) {
        return ApiResponse.success(service.directoryTree(dbId, path));
    }

    /**
     * 下载资源
     *
     * @param response
     * @param form
     */
    @Operation(summary = "下载资源")
    @GetMapping(value = "/download.do")
    public void downloadZip(HttpServletResponse response, ResourceForm form) {
        service.download(response, form);
    }
}
