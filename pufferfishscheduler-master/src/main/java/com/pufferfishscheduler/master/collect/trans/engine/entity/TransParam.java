package com.pufferfishscheduler.master.collect.trans.engine.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 转换参数
 * @author Mayc
 * @since 2025-09-07  01:02
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TransParam {
    private String name;
	private String value;
	private String defaultValue;
	private String description;
}
