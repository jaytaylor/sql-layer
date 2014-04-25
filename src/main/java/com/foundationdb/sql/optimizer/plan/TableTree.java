/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.ais.model.Table;

/** A subtree from the AIS.
 * In other words, a group.
 */
public class TableTree extends TableTreeBase<TableNode> 
{
    private int nbranches;

    protected TableNode createNode(Table table) {
        return new TableNode(table, this);
    }

    /** Determine branch sharing.
     * @return the number of branches. */
    public int colorBranches() {
        if (nbranches == 0)
            nbranches = colorBranches(root, 0);
        return nbranches;
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
