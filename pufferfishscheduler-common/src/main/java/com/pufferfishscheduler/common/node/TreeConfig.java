package com.pufferfishscheduler.common.node;

import lombok.Getter;

import java.util.Comparator;

/**
 * 树构建配置
 *
 * @author Mayc
 * @since 2025-11-25  16:49
 */
@Getter
public class TreeConfig<ID> {
    private String rootParentId;
    private boolean filterEmptyNodes = false;
    private Comparator<TreeNode<ID>> comparator;

    public static <ID> TreeConfig<ID> create() {
        return new TreeConfig<>();
    }

    public TreeConfig<ID> rootParentId(String rootParentId) {
        this.rootParentId = rootParentId;
        return this;
    }

    public TreeConfig<ID> filterEmptyNodes(boolean filterEmptyNodes) {
        this.filterEmptyNodes = filterEmptyNodes;
        return this;
    }

    public TreeConfig<ID> comparator(Comparator<TreeNode<ID>> comparator) {
        this.comparator = comparator;
        return this;
    }
}