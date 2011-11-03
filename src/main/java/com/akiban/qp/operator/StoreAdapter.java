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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;

public abstract class StoreAdapter
{
    public abstract GroupCursor newGroupCursor(GroupTable groupTable);

    public abstract Cursor newIndexCursor(Index index,
                                          boolean reverse,
                                          IndexKeyRange keyRange,
                                          IndexScanSelector scanSelector);

    public abstract HKey newHKey(RowType rowType);

    public final Schema schema()
    {
        return schema;
    }

    public abstract void updateRow(Row oldRow, Row newRow, Bindings bindings);
    
    public abstract void writeRow (Row newRow, Bindings bindings);
    
    public abstract void deleteRow (Row oldRow, Bindings bindings);

    public abstract Cursor sort(Cursor input, RowType rowType, API.Ordering ordering, Bindings bindings);

    public abstract void checkQueryCancelation(long queryStartMsec);

    // For use by subclasses

    protected StoreAdapter(Schema schema)
    {
        this.schema = schema;
    }

    // Object state

    protected final Schema schema;
}
