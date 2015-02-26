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

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.storeadapter.indexcursor.IndexCursor;
import com.geophile.z.Cursor;
import com.geophile.z.space.SpaceImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GeophileCursor extends Cursor<IndexRow>
{
    // Cursor interface

    @Override
    public IndexRow next() throws IOException, InterruptedException
    {
        if (currentCursor == null) {
            // A cursor should have been registered with z-value corresponding to the entire space,
            // Z_MIN = (0x0, 0).
            currentCursor = cursors.get(SpaceImpl.Z_MIN);
            assert currentCursor != null;
        }
        return currentCursor.next();
    }

    @Override
    public void goTo(IndexRow key) throws IOException, InterruptedException
    {
        long z = key.z();
        currentCursor = cursors.get(z);
        assert currentCursor != null : SpaceImpl.formatZ(z);
        if (!openEarly) {
            currentCursor.open(); // open is idempotent
        }
        currentCursor.jump(key, null);
    }

    @Override
    public boolean deleteCurrent() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    // GeophileCursor interface

    public void addCursor(long z, IndexCursor indexCursor)
    {
        if (currentCursor != null) {
            // Shouldn't add a cursor after the cursor has been opened.
            throw new IllegalArgumentException();
        }
        CachingCursor cachingCursor = new CachingCursor(z, indexCursor);
        if (openEarly) {
            cachingCursor.open();
        }
        cursors.put(z, cachingCursor);
    }

    public void rebind(QueryBindings bindings)
    {
        for (CachingCursor cursor : cursors.values()) {
            cursor.rebind(bindings);
        }
    }

    public void close()
    {
        for (CachingCursor cursor : cursors.values()) {
            cursor.close();
        }
    }

    public GeophileCursor(GeophileIndex index, boolean openEarly)
    {
        super(index);
        this.openEarly = openEarly;
    }

    // Object state

    private final boolean openEarly;
    private final Map<Long, CachingCursor> cursors = new HashMap<>(); // z -> CachingCursor
    private CachingCursor currentCursor;
}
