package com.pufferfishscheduler.master.collect.trans.plugin.service;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.domain.DebeziumField;

import java.util.List;

public interface DebeziumPluginService {

    /**
     * 结构报文数据
     *
     * @param sample
     */
    List<DebeziumField> parseSampleData(JSONObject sample);
}
