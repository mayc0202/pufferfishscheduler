package com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson2.JSONArray;
import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * 运行时配置
 * @author Mayc
 * @since 2026-03-13 17:53
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RuntimeConfig {
    
    /**
     * 源Connector名称
     */
    private String sourceConnectorName;

    /**
     * 目标连接器名称
     */
    private List<String> targetConnectorNames;

    /**
     * 服务名称
     */
    private String serverName;

    /**
     * 原表在kafka中的主题
     */
    private List<String> topics;

    /**
     * 统计任务名称
     */
    private String statsTaskName;

    public RuntimeConfig(String config) {
        if (StringUtils.isEmpty(config)) {
            return;
        }

        JSONObject jsonObject = JSONObject.parseObject(config);
        this.sourceConnectorName = jsonObject.getString("sourceConnectorName");
        JSONArray ja = jsonObject.getJSONArray("targetConnectorNames");

        if(ja!=null && ja.size()>0){
            List<String> targetConnectorNames = new ArrayList<>();
            for(int i=0;i<ja.size();i++) {
                targetConnectorNames.add(ja.getString(i));
            }

            this.targetConnectorNames = targetConnectorNames;
        }

        this.serverName = jsonObject.getString("serverName");
        this.statsTaskName = jsonObject.getString("statsTaskName");

        JSONArray topicArr = jsonObject.getJSONArray("topics");
        if(topicArr !=null && topicArr.size()>0) {
            List<String> topics = new ArrayList<>();
            for(int i=0;i<topicArr.size();i++) {
                topics.add(topicArr.getString(i));
            }

            this.topics =topics;
        }
    }
}
