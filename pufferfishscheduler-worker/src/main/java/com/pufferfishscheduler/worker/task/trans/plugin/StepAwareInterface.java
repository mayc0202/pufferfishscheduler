package com.pufferfishscheduler.worker.task.trans.plugin;

import com.pufferfishscheduler.worker.task.trans.engine.entity.TransParam;

import java.util.List;

public interface StepAwareInterface {
    void beforeStep(Integer flowId, String stepId, String stepConfig, List<TransParam> params);

    void afterStep(Integer flowId, String stepId, String stepConfig);
}
