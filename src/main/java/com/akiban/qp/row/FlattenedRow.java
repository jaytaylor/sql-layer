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

package com.akiban.qp.row;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.FlattenedRowType;

public class FlattenedRow extends CompoundRow
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s, %s", first(), second());
    }

    // Row interface

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable)
    {
        return     (first().isHolding() && first().get().rowType().hasUserTable() && first().get().rowType().userTable() == userTable)
                   || (second().isHolding() && second().get().rowType().hasUserTable() && second().get().rowType().userTable() == userTable)
                   || (first().isHolding() && first().get().containsRealRowOf(userTable))
                   || (second().isHolding() && second().get().containsRealRowOf(userTable))
            ;
    }

    // FlattenedRow interface

    public FlattenedRow(FlattenedRowType rowType, Row parent, Row child, HKey hKey)
    {
        super (rowType, parent, child);
        this.hKey = hKey;
        if (parent != null && child != null) {
            // assert parent.runId() == child.runId();
        }
        if (parent != null && !rowType.parentType().equals(parent.rowType())) {
            throw new IllegalArgumentException("mismatched type between " +rowType+ " and parent " + parent.rowType());
        }
    }

    // Object state

    private final HKey hKey;
}
