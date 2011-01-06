package com.akiban.cserver.service.tree;

import com.persistit.Tree;

public class TreeCache {
    private final Tree tree;
    private int tableIdOffset = -1;

    TreeCache(final Tree tree) {
        this.tree = tree;
    }

    /**
     * @return the tableIdOffset
     */
    public int getTableIdOffset() {
        return tableIdOffset;
    }

    /**
     * @param tableIdOffset
     *            the tableIdOffset to set
     */
    public void setTableIdOffset(int tableIdOffset) {
        this.tableIdOffset = tableIdOffset;
    }

    /**
     * @return the tree
     */
    public Tree getTree() {
        return tree;
    }
}