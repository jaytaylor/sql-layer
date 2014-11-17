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

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.storeadapter.indexcursor.IndexCursor;
import com.geophile.z.Cursor;
import com.geophile.z.Index;

import java.io.IOException;

class FDBIndexToGeophileCursorAdapter extends Cursor<Row>
{
    // com.geophile.z.Cursor interface

    @Override
    public Row next() throws IOException, InterruptedException
    {
        return indexCursor.next();
    }

    @Override
    public Row previous() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void goTo(Row key) throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteCurrent() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    // FDBIndexToGeophileCursorAdapter interface

    public FDBIndexToGeophileCursorAdapter(Index<Row> index, IndexCursor indexCursor)
    {
        super(index);
        this.indexCursor = indexCursor;
    }

    // Object state

    private final IndexCursor indexCursor;
}
