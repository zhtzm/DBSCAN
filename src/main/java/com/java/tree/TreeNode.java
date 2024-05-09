package com.java.tree;

public class TreeNode {
    private int split;
    private Double[] bottom;
    private Double[] top;
    private long count;
    private TreeNode left;
    private TreeNode right;

    public int getSplit() {
        return split;
    }

    public void setSplit(int split) {
        this.split = split;
    }

    public Double[] getBottom() {
        return bottom;
    }

    public void setBottom(Double[] bottom) {
        this.bottom = bottom;
    }

    public Double[] getTop() {
        return top;
    }

    public void setTop(Double[] top) {
        this.top = top;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public TreeNode getLeft() {
        return left;
    }

    public void setLeft(TreeNode left) {
        this.left = left;
    }

    public TreeNode getRight() {
        return right;
    }

    public void setRight(TreeNode right) {
        this.right = right;
    }

    // 构造函数
    public TreeNode(int val, Double[] min, Double[] max, long num) {
        split = val;
        bottom = min;
        top = max;
        count = num;
        left = null;
        right = null;
    }
}
