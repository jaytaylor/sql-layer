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

import com.akiban.ais.model.Index;
import com.akiban.server.IndexDef;

public class IndexRowType extends RowType
{
    // Object interface

    @Override
    public String toString()
    {
        return index.toString();
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return ((IndexDef)index.indexDef()).indexKeyFields().length;
    }

    @Override
    public boolean ancestorOf(RowType type)
    {
        assert false : "Not implemented yet";
        return false;
    }

    // IndexRowType interface

    public Index index()
    {
        return index;
    }

    public IndexRowType(Schema schema, Index index, int typeId)
    {
        super(schema, typeId, null);
        this.index = index;
    }

    // Object state

    private final Index index;
}
