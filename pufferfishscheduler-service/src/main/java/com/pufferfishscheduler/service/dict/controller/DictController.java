package com.pufferfishscheduler.service.dict.controller;

import com.pufferfishscheduler.service.dict.service.DictService;
import com.pufferfishscheduler.common.result.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    @GetMapping("/getDict.do")
    public ApiResponse getDict(@RequestParam String dictCode) {
        return ApiResponse.success(dictService.getDict(dictCode));
    }
}
