package com.pufferfishscheduler.common.result;

import lombok.Data;

/**
 * 测试数据源是否能够连接
 */
@Data
public class ConResponse {

    private Boolean result;

    private String msg;

}
