package com.pufferfishscheduler.common.node;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 树形结构信息
 */
@Data
public class Tree implements TreeNode<Integer> {

    private Integer id;
    private String name;
    private String type;
    private Integer parentId;
    private String treeParentId;
    private String treeId;
    private String icon;
    private Integer orderBy;
    private List<TreeNode<Integer>> children = new ArrayList<>();

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public Integer getParentId() {
        return parentId;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public Integer getOrder() {
        return orderBy;
    }

    @Override
    public List<TreeNode<Integer>> getChildren() {
        return children;
    }

    @Override
    public void setChildren(List<TreeNode<Integer>> children) {
        this.children = children != null ? children : new ArrayList<>();
    }

    // 便捷方法：添加子节点
    public void addChild(TreeNode<Integer> child) {
        this.children.add(child);
    }

//    // 便捷方法：获取Tree类型的子节点
//    public List<Tree> getTreeChildren() {
//        List<Tree> treeChildren = new ArrayList<>();
//        for (TreeNode<Integer> node : children) {
//            if (node instanceof Tree) {
//                treeChildren.add((Tree) node);
//            }
//        }
//        return treeChildren;
//    }
}