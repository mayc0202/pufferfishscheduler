package com.pufferfishscheduler.master.common.dict.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import com.pufferfishscheduler.master.common.dict.service.DictService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
/**
 * 字典控制层
 *
 * @author mayc
 * @since 2026-03-04
 */
@Tag(name = OpenApiTags.DICT,description = OpenApiTags.DICT_DESC)
@RestController
@RequestMapping(value = "/dict", produces = {"application/json;charset=utf-8"})
public class DictController {

    @Autowired
    private DictService dictService;

    /**
     * 获取数据源分层字典
     *
     * @param dictCode 字典编码
     * @return 字典项列表
     */
    @Operation(summary = "获取数据源分层字典")
    @GetMapping("/getDict.do")
    public ApiResponse getDict(@RequestParam String dictCode) {
        return ApiResponse.success(dictService.getDict(dictCode));
    }
}
