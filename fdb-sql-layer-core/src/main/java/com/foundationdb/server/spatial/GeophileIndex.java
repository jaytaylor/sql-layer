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

package com.foundationdb.server.spatial;

import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.geophile.z.Cursor;
import com.geophile.z.DuplicateRecordException;
import com.geophile.z.Index;
import com.geophile.z.RecordFilter;

import java.io.IOException;

public class GeophileIndex extends Index<Row>
{
    // Index interface

    @Override
    public void add(Row record) throws IOException, InterruptedException, DuplicateRecordException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(long z, RecordFilter<Row> recordFilter) throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<Row> cursor() throws IOException, InterruptedException
    {
        return cursorFactory.newCursor(this);
    }

    @Override
    public Row newRecord()
    {
        return adapter.takeIndexRow(indexRowType);
    }

    @Override
    public boolean blindUpdates()
    {
        return false;
    }

    @Override
    public boolean stableRecords()
    {
        return true;
    }

    // GeophileIndex interface

    public GeophileIndex(StoreAdapter adapter, IndexRowType indexRowType, CursorFactory cursorFactory)
    {
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        this.cursorFactory = cursorFactory;
    }

    // Object state

    private final StoreAdapter adapter;
    private final IndexRowType indexRowType;
    private final CursorFactory cursorFactory;

    public interface CursorFactory
    {
        public GeophileCursor newCursor(GeophileIndex geophileIndex);
    }
}
