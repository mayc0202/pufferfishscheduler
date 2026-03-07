package com.pufferfishscheduler.master.collect.trans.plugin;

import java.util.List;

import com.pufferfishscheduler.master.collect.trans.engine.entity.TransParam;

public interface StepAwareInterface {
    void beforeStep(Integer flowId, String stepId, String stepConfig, List<TransParam> params);

    void afterStep(Integer flowId, String stepId, String stepConfig);
}
