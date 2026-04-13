package com.pufferfishscheduler.domain.vo.collect;

import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 行转列字段
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DenormalizedFieldVo {

    private String fieldName;

    private String keyValue;

    @Size(max = 100,message = "输出字段名称不能超过100个字符！")
    private String targetName;
}
