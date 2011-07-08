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

package com.akiban.qp.physicaloperator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.SchemaOBSOLETE;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;

public abstract class StoreAdapter
{
    public abstract GroupCursor newGroupCursor(GroupTable groupTable);

    public final Cursor newIndexCursor(TableIndex index)
    {
        return newIndexCursor(index, false, null);
    }

    public abstract Cursor newIndexCursor(Index index, boolean reverse, IndexKeyRange keyRange);

    public abstract HKey newHKey(RowType rowType);

    public final SchemaOBSOLETE schema()
    {
        return schema;
    }

    public abstract void updateRow(Row oldRow, Row newRow, Bindings bindings);

    public abstract RowData rowData(RowDef rowDef, RowBase row, Bindings bindings);

    // For use by subclasses

    protected StoreAdapter(SchemaOBSOLETE schema)
    {
        this.schema = schema;
    }

    // Object state

    protected final SchemaOBSOLETE schema;
}
