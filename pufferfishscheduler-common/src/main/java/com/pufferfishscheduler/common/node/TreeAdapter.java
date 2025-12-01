package com.pufferfishscheduler.common.node;

/**
 * 树节点适配器接口
 *
 * @author Mayc
 * @since 2025-11-25  16:51
 */
public interface TreeAdapter<T, R extends TreeNode<?>> {
    R adapt(T entity);
}
