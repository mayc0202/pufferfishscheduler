package com.pufferfishscheduler.master.collect.trans.plugin.controller;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.collect.trans.plugin.service.FtpPluginService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * FTP 插件控制器
 *
 * @author mayc
 */
@Tag(name = OpenApiTags.TRANS_FLOW_COMPONENT, description = OpenApiTags.TRANS_FLOW_COMPONENT_DESC)
@Validated
@RestController
@RequestMapping("/plugin/ftp")
public class FtpPluginController {


    @Autowired
    private FtpPluginService ftpPluginService;

    /**
     * 获取FTP数据源树形结构
     *
     * @return
     */
    @Operation(summary = "获取FTP数据源树形结构")
    @GetMapping("/ftpDbTree.do")
    public ApiResponse ftpDbTree() {
        return ApiResponse.success(ftpPluginService.ftpDbTree());
    }

    /**
     * 获取本地目录下的文件
     *
     * @param path 本地目录路径
     * @return 文件列表
     */
    @Operation(summary = "获取本地目录下的文件")
    @GetMapping("/getLocalDirectory.do")
    public ApiResponse getLocalDirectory(@RequestParam String path) {
        return ApiResponse.success(ftpPluginService.getLocalDirectory(path, Constants.LOCAL_FILE_TYPE.DIR));
    }
}
