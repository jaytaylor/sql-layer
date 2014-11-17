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
import com.foundationdb.qp.rowtype.IndexRowType;
import com.geophile.z.Cursor;
import com.geophile.z.DuplicateRecordException;
import com.geophile.z.Index;
import com.geophile.z.Record;
import com.geophile.z.RecordFilter;

import java.io.IOException;

public class GeophileIndex<RECORD extends Record> extends Index<RECORD>
{
    // Index interface

    @Override
    public void add(RECORD record) throws IOException, InterruptedException, DuplicateRecordException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(long z, RecordFilter<RECORD> recordFilter) throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<RECORD> cursor() throws IOException, InterruptedException
    {
        return new GeophileCursor<>(this, openCursorsEarly);
    }

    @Override
    public RECORD newRecord()
    {
        assert false;
        return null;
/*
        return (RECORD) adapter.takeIndexRow(indexRowType);
*/
    }

    // GeophileIndex interface

    public GeophileIndex(StoreAdapter adapter, IndexRowType indexRowType, boolean openCursorsEarly)
    {
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        this.openCursorsEarly = openCursorsEarly;
    }

    // Object state

    private final StoreAdapter adapter;
    private final IndexRowType indexRowType;
    private final boolean openCursorsEarly;
}
