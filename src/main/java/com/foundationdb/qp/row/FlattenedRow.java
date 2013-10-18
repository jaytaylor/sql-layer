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

package com.foundationdb.qp.row;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.rowtype.FlattenedRowType;

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
    public boolean containsRealRowOf(Table table)
    {
        return     (first() != null && first().rowType().hasTable() && first().rowType().table() == table)
                   || (second() != null && second().rowType().hasTable() && second().rowType().table() == table)
                   || (first() != null && first().containsRealRowOf(table))
                   || (second() != null && second().containsRealRowOf(table))
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
