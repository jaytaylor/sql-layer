package com.foundationdb.server.spatial;

import com.foundationdb.qp.operator.CursorBase;
import com.foundationdb.qp.storeadapter.indexcursor.IndexCursor;
import com.geophile.z.Cursor;
import com.geophile.z.Record;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GeophileCursor<RECORD extends Record> extends Cursor<RECORD>
{
    // Cursor interface

    @Override
    public RECORD next() throws IOException, InterruptedException
    {
        if (currentCursor == null) {
            // A cursor should have been registered with z-value (0x0, 0).
            currentCursor = cursors.get(0L);
            assert currentCursor != null;
        }
        return currentCursor.next();
    }

    @Override
    public RECORD previous() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void goTo(RECORD key) throws IOException, InterruptedException
    {
        long z = key.z();
        currentCursor = cursors.get(z);
        assert currentCursor != null : z;
        if (!openEarly) {
            currentCursor.open(); // open is idempotent
        }
        currentCursor.goTo(key);
    }

    @Override
    public boolean deleteCurrent() throws IOException, InterruptedException
    {
        return false;
    }

    // GeophileCursor interface

    public void addCursor(long z, IndexCursor indexCursor)
    {
        if (currentCursor != null) {
            // Shouldn't add a cursor after the cursor has been opened.
            throw new IllegalArgumentException();
        }
        CachingCursor<RECORD> cachingCursor = new CachingCursor<>(index, z, (CursorBase) indexCursor);
        if (openEarly) {
            cachingCursor.open();
        }
        cursors.put(z, cachingCursor);
    }

    public void close()
    {
        for (CachingCursor<RECORD> cursor : cursors.values()) {
            cursor.close();
        }
    }

    public GeophileCursor(GeophileIndex<RECORD> index, boolean openEarly)
    {
        super(index);
        this.index = index;
        this.openEarly = openEarly;
        if (openEarly) {
            for (Map.Entry<Long, CachingCursor<RECORD>> entry : cursors.entrySet()) {
                long z = entry.getKey();
                CachingCursor<RECORD> cursor = entry.getValue();
                cursor.open();
                if (z == 0L) {
                    currentCursor = cursor;
                }
            }
        }
    }

    // Object state

    private final GeophileIndex<RECORD> index;
    private final boolean openEarly;
    private final Map<Long, CachingCursor<RECORD>> cursors = new HashMap<>(); // z -> CachingCursor
    private CachingCursor<RECORD> currentCursor;
}
