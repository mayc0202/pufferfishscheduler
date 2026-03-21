package com.pufferfishscheduler.common.node;

import com.pufferfishscheduler.common.constants.Constants;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 通用树构建器
 *
 * @author Mayc
 * @since 2025-11-25  16:49
 */
@Component
public class GenericTreeBuilder {
    /**
     * 构建树形结构
     */
    public List<Tree> buildTree(List<Tree> allNodes) {
        return buildTree(allNodes, null);
    }

    /**
     * 构建树形结构（带排序）
     */
    public List<Tree> buildTree(List<Tree> allNodes, Comparator<TreeNode<Integer>> comparator) {
        if (CollectionUtils.isEmpty(allNodes)) {
            return Collections.emptyList();
        }

        // 按父ID分组
        Map<Integer, List<Tree>> parentChildMap = allNodes.stream()
                .filter(node -> node.getParentId() != null)
                .collect(Collectors.groupingBy(Tree::getParentId));

        // 获取根节点
        List<Tree> roots = allNodes.stream()
                .filter(this::isRootNode)
                .collect(Collectors.toList());

        // 为每个根节点构建子树（每棵树使用独立的 visiting 集合，防止父子形成环导致递归死循环）
        for (Tree root : roots) {
            buildTreeRecursive(root, parentChildMap, comparator, new HashSet<>());
        }

        // 排序根节点
        if (comparator != null) {
            roots.sort(comparator);
        }

        return roots;
    }

    /**
     * 递归构建树（带环检测，使用当前递归路径上的 visiting 集合）
     */
    private void buildTreeRecursive(Tree parent,
                                    Map<Integer, List<Tree>> parentChildMap,
                                    Comparator<TreeNode<Integer>> comparator,
                                    Set<Integer> visiting) {
        if (parent == null || parent.getId() == null) {
            return;
        }

        Integer parentId = parent.getId();

        // 检测环：若当前节点已经在本次递归路径中，说明存在环，直接返回，避免 StackOverflow
        if (!visiting.add(parentId)) {
            return;
        }

        List<Tree> children = parentChildMap.get(parentId);
        if (CollectionUtils.isEmpty(children)) {
            // 叶子节点，递归结束，移除当前路径上的访问标记
            visiting.remove(parentId);
            return;
        }

        // 设置子节点
        parent.setChildren(new ArrayList<>(children));

        // 递归构建子树
        for (Tree child : children) {
            // 防止父子记录相同 ID 导致自递归
            if (child != null && !Objects.equals(child.getId(), parentId)) {
                buildTreeRecursive(child, parentChildMap, comparator, visiting);
            }
        }

        // 排序子节点
        if (comparator != null) {
            parent.getChildren().sort(comparator);
        }

        // 当前节点递归结束，移除访问标记
        visiting.remove(parentId);
    }

    private boolean isRootNode(Tree node) {
        Integer parentId = node.getParentId();
        return parentId == null || parentId == 0;
    }

    /**
     * 过滤空分组
     */
    public List<Tree> filterEmptyGroups(List<Tree> nodes) {
        List<Tree> result = new ArrayList<>();
        for (Tree node : nodes) {
            Tree filteredNode = filterNode(node);
            if (filteredNode != null) {
                result.add(filteredNode);
            }
        }
        return result;
    }

    /**
     * 过滤节点
     *
     * @param node
     * @return
     */
    private Tree filterNode(Tree node) {
        // 如果不是分组节点，直接返回
        if (!Constants.TREE_TYPE.GROUP.equals(node.getType())) {
            return node;
        }

        // 过滤子节点
        List<Tree> filteredChildren = new ArrayList<>();
        for (TreeNode<Integer> child : node.getChildren()) {
            if (child instanceof Tree) {
                Tree filteredChild = filterNode((Tree) child);
                if (filteredChild != null) {
                    filteredChildren.add(filteredChild);
                }
            }
        }

        // 设置过滤后的子节点
        node.setChildren(new ArrayList<>(filteredChildren));

        // 判断是否应该过滤掉该节点
        if (shouldFilterOut(node)) {
            return null;
        }

        return node;
    }

    private boolean shouldFilterOut(Tree node) {
        if (!Constants.TREE_TYPE.GROUP.equals(node.getType())) {
            return false;
        }

        if (node.getChildren().isEmpty()) {
            return true;
        }

        // 检查是否所有子节点都是空分组
        for (TreeNode<Integer> child : node.getChildren()) {
            if (child instanceof Tree) {
                Tree treeChild = (Tree) child;
                if (!Constants.TREE_TYPE.GROUP.equals(treeChild.getType()) ||
                        !treeChild.getChildren().isEmpty()) {
                    return false;
                }
            } else {
                return false; // 有非Tree类型的子节点，不过滤
            }
        }

        return true;
    }
}
