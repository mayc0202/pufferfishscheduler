package com.pufferfishscheduler.common.enums;

import lombok.Getter;

/**
 * 连接方式
 */
@Getter
public enum ConnectionType {
    DIRECT, // 单节点
    CLUSTER // 集群
}
