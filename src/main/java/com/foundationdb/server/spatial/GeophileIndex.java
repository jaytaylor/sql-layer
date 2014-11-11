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
        return (RECORD) adapter.takeIndexRow(indexRowType);
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
