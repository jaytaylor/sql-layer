/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
