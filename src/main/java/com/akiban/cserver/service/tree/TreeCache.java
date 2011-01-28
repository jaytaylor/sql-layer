/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

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