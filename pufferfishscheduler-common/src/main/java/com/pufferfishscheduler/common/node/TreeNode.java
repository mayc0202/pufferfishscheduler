package com.pufferfishscheduler.common.node;

import java.util.List;

/**
 * 树形节点
 *
 * @author Mayc
 * @since 2025-11-25  16:48
 */
public interface TreeNode<ID> {
    ID getId();
    ID getParentId();
    String getName();
    String getType();
    Integer getOrder();
    List<TreeNode<ID>> getChildren();
    void setChildren(List<TreeNode<ID>> children);
}
