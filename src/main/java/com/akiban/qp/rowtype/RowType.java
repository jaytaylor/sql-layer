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

// Fields are untyped for now. Field name is just position within the type.

public abstract class RowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("RowType(%s)", typeId);
    }

    @Override
    public int hashCode()
    {
        return typeId * 9987001;
    }

    @Override
    public boolean equals(Object o)
    {
        return o != null && o instanceof RowType && this.typeId == ((RowType)o).typeId;
    }

    // RowType interface

    public final Schema schema()
    {
        return schema;
    }

    public final int typeId()
    {
        return typeId;
    }

    public final Ancestry ancestry()
    {
        return ancestry;
    }

    public abstract int nFields();

    public abstract boolean ancestorOf(RowType type);

    // For use by subclasses

    protected RowType(Schema schema, int typeId, Ancestry ancestry)
    {
        this.schema = schema;
        this.typeId = typeId;
        this.ancestry = ancestry;
    }

    // Object state

    private final Schema schema;
    private final int typeId;
    private final Ancestry ancestry;
}
