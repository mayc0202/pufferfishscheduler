package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessBranchSetupVo {

    String caseValue;

    String stepId;
}
