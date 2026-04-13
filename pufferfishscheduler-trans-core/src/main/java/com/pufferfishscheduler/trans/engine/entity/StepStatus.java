package com.pufferfishscheduler.trans.engine.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 步骤状态
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StepStatus {
    private String stepName;
	private String status;  // S-执行成功；F-执行失败；R-执行中
}
