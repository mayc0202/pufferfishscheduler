package com.pufferfishscheduler.common.dict.controller;

import com.pufferfishscheduler.common.dict.service.DictService;
import com.pufferfishscheduler.common.result.ApiResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "字典管理")
@RestController
@RequestMapping(value = "/dict", produces = {"application/json;charset=utf-8"})
public class DictController {

    @Autowired
    private DictService dictService;

    /**
     * 获取数据源分层字典
     *
     * @return
     */
    @ApiOperation(value = "获取字典")
    @GetMapping("/getDict.do")
    public ApiResponse getDict(@RequestParam String dictCode) {
        return ApiResponse.success(dictService.getDict(dictCode));
    }
}
