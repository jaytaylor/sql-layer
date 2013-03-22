
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
