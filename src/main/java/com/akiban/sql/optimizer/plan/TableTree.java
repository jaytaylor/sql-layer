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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.UserTable;

/** A subtree from the AIS.
 * In other words, a group.
 */
public class TableTree extends TableTreeBase<TableNode> 
{
    protected TableNode createNode(UserTable table) {
        return new TableNode(table, this);
    }

    /** Determine branch occurrence.
     * @return the number of branches. */
    public int colorBranches() {
        return colorBranches(root, 0);
    }

    private int colorBranches(TableNode node, int nbranches) {
        long branches = 0;
        for (TableNode child = node.getFirstChild(); 
             child != null; 
             child = child.getNextSibling()) {
            nbranches = colorBranches(child, nbranches);
            // A parent is on the same branch as any child.
            branches |= child.getBranches();
        }
        if (branches == 0) {
            // The leaf of a new branch.
            branches = (1L << nbranches++);
        }
        node.setBranches(branches);
        return nbranches;
    }

}
