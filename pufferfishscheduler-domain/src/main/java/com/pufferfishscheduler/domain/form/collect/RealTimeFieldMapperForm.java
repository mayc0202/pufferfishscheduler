package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * 实时数据同步表字段映射表单
 */
@Data
public class RealTimeFieldMapperForm {

    private Integer id;

    /**
     * 来源字段ID
     */
    @NotBlank(message = "来源字段ID不能为空!")
    private Integer sourceFieldId;

    /**
     * 源字段名称
     */
    @NotBlank(message = "源字段名称不能为空!")
    private String sourceFieldName;

    /**
     * 目标字段ID
     */
    @NotBlank(message = "目标字段ID不能为空!")
    private Integer targetFieldId;

    /**
     * 目标字段名称
     */
    @NotBlank(message = "目标字段名称不能为空!")
    private String targetFieldName;

}
