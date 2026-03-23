package com.pufferfishscheduler.worker.task.trans.plugin;

import lombok.Data;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Map;

/**
 * 步骤上下文
 */
@Data
public class StepContext {
     private PluginRegistry registryID;
    private Map<String, StepMeta> stepMetaMap;
    //组件ID
    private String id;
    //是否需要校验组件
    private boolean validate;
    /**
     * 流程id
     */
    private Integer flowId;

    private String url;

    private String apiKey;

    private String rootPath;

    private String workDir;

    private String address;

    private String databaseUrl;

    private String username;

    private String password;

    //组件id及步骤名称
    private Map<String, String> stepNames;
}
