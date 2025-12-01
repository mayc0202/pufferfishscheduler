package com.pufferfishscheduler.domain.form.metadata;

import lombok.Data;

import javax.validation.constraints.NotNull;

/**
 * 编辑表单
 *
 * @author Mayc
 * @since 2025-12-01  18:39
 */
@Data
public class MetadataTaskUpdateForm extends MetadataTaskForm {

    @NotNull(message = "任务id不能为空!")
    private Integer id;
}
