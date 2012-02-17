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

package com.akiban.qp.rowtype;


import com.akiban.ais.model.UserTable;
import com.akiban.util.ArgumentValidation;

import java.util.*;

public class SingleBranchTypeComposition extends TypeComposition
{
    /**
     * Indicates whether this is an ancestor of that: this is identical to that, or:
     * - the tables comprising this and that are disjoint, and
     * - the rootmost table of that has an ancestor among the tables of this.
     * @param typeComposition
     * @return true if this is an ancestor of that, false otherwise.
     */
    public boolean isAncestorOf(TypeComposition typeComposition)
    {
        Boolean ancestor;
        SingleBranchTypeComposition that = (SingleBranchTypeComposition) typeComposition;
        if (this == that) {
            ancestor = Boolean.TRUE;
        } else {
            ancestor = ancestorOf.get(that.rowType);
            if (ancestor == null) {
                // Check for tables in common
                ancestor = Boolean.TRUE;
                for (UserTable table : that.tables) {
                    if (this.tables.contains(table)) {
                        ancestor = Boolean.FALSE;
                    }
                }
                if (ancestor) {
                    ancestor = levelsApart(that) > 0;
                }
                ancestorOf.put(that.rowType, ancestor);
            }
        }
        return ancestor;
    }

    /**
     * Indicates whether this is a parentof that: this is not identical to that, and:
     * - the tables comprising this and that are disjoint, and
     * - the rootmost table's parent is among the tables of this.
     * @param typeComposition
     * @return true if this is an ancestor of that, false otherwise.
     */
    public boolean isParentOf(TypeComposition typeComposition)
    {
        boolean ancestor;
        SingleBranchTypeComposition that = (SingleBranchTypeComposition) typeComposition;
        if (this == that) {
            ancestor = false;
        } else {
            ancestor = isAncestorOf(that);
            if (ancestor) {
                ancestor = levelsApart(that) > 0;
            }
        }
        return ancestor;
    }

    public SingleBranchTypeComposition(RowType rowType, UserTable table)
    {
        this(rowType, Arrays.asList(table));
    }

    public SingleBranchTypeComposition(RowType rowType, Collection<UserTable> tables)
    {
        super(rowType, tables);
    }

    // For use by this class

    // If this is an ancestor of that, then the return value is the number of generations separating the two.
    // (parent = 1). If this is not an ancestor of that, return -1.
    public int levelsApart(SingleBranchTypeComposition that)
    {
        // Find rootmost table in that
        UserTable thatRoot = that.tables.iterator().next();
        while (thatRoot.parentTable() != null && that.tables.contains(thatRoot.parentTable())) {
            thatRoot = thatRoot.parentTable();
        }
        // this is an ancestor of that if that's rootmost table has an ancestor in this.
        int generationsApart = 0;
        UserTable thatAncestor = thatRoot;
        boolean ancestor = false;
        while (thatAncestor != null && !ancestor) {
            thatAncestor = thatAncestor.parentTable();
            ancestor = this.tables.contains(thatAncestor);
            generationsApart++;
        }
        return ancestor ? generationsApart : -1;
    }

    // Object state

    private final Map<RowType, Boolean> ancestorOf = new HashMap<RowType, Boolean>();
}
