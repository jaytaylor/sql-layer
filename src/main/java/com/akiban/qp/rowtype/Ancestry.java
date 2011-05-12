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
import com.akiban.server.RowDef;

public class Ancestry
{
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('[');
        boolean first = true;
        for (int typeId : typeIds) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(typeId);
        }
        buffer.append(']');
        return buffer.toString();
    }

    public int commonSegments(Ancestry that)
    {
        int n = Math.min(this.typeIds.length, that.typeIds.length);
        int s = 0;
        while (s < n && this.typeIds[s] == that.typeIds[s]) {
            s++;
        }
        return s;
    }

    public static Ancestry of(UserTable table)
    {
        int[] typeIds = new int[table.getDepth() + 1];
        while (table != null) {
            RowDef rowDef = (RowDef) table.rowDef();
            typeIds[table.getDepth()] = rowDef.getOrdinal();
            table = table.parentTable();
        }
        return new Ancestry(typeIds);
    }

    public Ancestry(int... typeIds)
    {
        this(typeIds.length);
        System.arraycopy(typeIds, 0, this.typeIds, 0, typeIds.length);
    }

    // For use by this class

    private Ancestry(int n)
    {
        typeIds = new int[n];
    }

    // Object state

    private final int[] typeIds;
}
