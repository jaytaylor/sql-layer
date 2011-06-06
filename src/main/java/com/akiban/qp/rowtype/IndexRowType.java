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
import com.akiban.ais.model.TableIndex;
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
        return index.getColumns().size();
    }

    // IndexRowType interface

    public UserTableRowType tableType()
    {
        return tableType;
    }

    public IndexKeyType keyType()
    {
        return keyType;
    }

    public TableIndex index()
    {
        return index;
    }

    public IndexRowType(Schema schema, UserTableRowType tableType, TableIndex index)
    {
        super(schema, schema.nextTypeId());
        this.tableType = tableType;
        this.index = index;
        this.keyType = new IndexKeyType(schema, index);
    }

    // Object state

    private final UserTableRowType tableType;
    private final TableIndex index;
    private final IndexKeyType keyType;
}
