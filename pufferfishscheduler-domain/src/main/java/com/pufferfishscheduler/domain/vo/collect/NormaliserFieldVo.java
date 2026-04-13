package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 列传行字段
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class NormaliserFieldVo {

    private String name;

    private String value;
}
