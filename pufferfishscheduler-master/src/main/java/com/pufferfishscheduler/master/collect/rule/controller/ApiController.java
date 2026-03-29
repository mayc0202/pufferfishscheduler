package com.pufferfishscheduler.master.collect.rule.controller;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.collect.rule.service.RuleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * 清洗规则API控制器
 * 为清洗组件提供清洗规则相关的API接口
 */
@RestController
@RequestMapping("/api/rule")
public class ApiController {

    @Autowired
    private RuleService ruleService;

    /**
     * 获取规则信息(已发布)
     *
     * @param ruleIds 规则ID列表
     * @return 规则信息列表
     */
    @PostMapping("/getRuleInformation.do")
    public ApiResponse getParamsConfig(@RequestBody(required = false) String ruleIds) {
        JSONArray ids = parseRuleIds(ruleIds);
        return ApiResponse.success(ruleService.getRuleInformation(ids));
    }

    /**
     * 支持两种请求体：
     * 1) ["id1","id2"]
     * 2) {"ruleId":["id1","id2"]}
     */
    private JSONArray parseRuleIds(String requestBody) {
        if (requestBody == null || requestBody.isBlank()) {
            return new JSONArray();
        }
        String body = requestBody.trim();

        if (body.startsWith("[")) {
            return JSONArray.parseArray(body);
        }
        if (body.startsWith("{")) {
            JSONObject obj = JSONObject.parseObject(body);
            if (obj == null) {
                return new JSONArray();
            }
            JSONArray arr = obj.getJSONArray("ruleId");
            return arr == null ? new JSONArray() : arr;
        }

        throw new IllegalArgumentException("请求体格式错误：应为数组或包含 ruleId 数组的对象");
    }

}
