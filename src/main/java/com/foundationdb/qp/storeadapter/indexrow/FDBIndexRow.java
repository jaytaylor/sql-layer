package com.foundationdb.qp.storeadapter.indexrow;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.util.HKeyCache;
import com.foundationdb.qp.util.PersistitKey;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.ArgumentValidation;
import com.persistit.Key;
import com.persistit.Value;

public class FDBIndexRow extends IndexRow {

    @Override
    public IndexRowType rowType()
    {
        return indexRowType;
    }

    @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public HKey ancestorHKey(Table table)
    {
        return new PersistitHKey(keyCreator.createKey(), table.hKey());
    }
    
    @Override
    public void initialize(RowData rowData, Key hKey,
            SpatialColumnHandler spatialColumnHandler, long zValue) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();

    }

    @Override
    public <S> void append(S source, TInstance type) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();

    }

    @Override
    public void append(EdgeValue value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();

    }

    @Override
    public void close(Session session, Store store, boolean forInsert) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();

    }

    @Override
    public void resetForRead(Index index, Key key, Value value) {
        // TODO Auto-generated method stub
        this.index = index;
        this.keyState = key;
        this.value = value;
    }

    @Override
    public void resetForWrite(Index index, Key createKey, Value value) {
        // TODO Auto-generated method stub
        this.index = index;
        this.keyState = createKey;
        this.value = value;
                
    }

    @Override
    public int compareTo(IndexRow startKey, int startBoundColumns,
            boolean[] ascending) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(IndexRow thatKey, int startBoundColumns) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
        pKeyAppends = 0;
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    
    @Override
    public void copyPersistitKeyTo(Key key)
    {
        keyState.copyTo(key);
    }
    
    @Override
    public void appendFieldTo(int position, Key target) {
        if (position < pKeyFields) {
            PersistitKey.appendFieldFromKey(target, keyState, position, index.getIndexName());
        //} else {
        //    PersistitKey.appendFieldFromKey(target, value, position - pKeyFields, index.getIndexName());
        }
        pKeyAppends++;
    }

    @Override
    public void copyFrom(Key key, Value value) {
        key.copyTo(keyState);
    }
    
    @Override
    public ValueSource value(int i) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public FDBIndexRow (KeyCreator keyCreator)
    {
        ArgumentValidation.notNull("keyCreator", keyCreator);
        this.keyCreator = keyCreator;
        this.indexRowType = null;
    }

    public FDBIndexRow (KeyCreator adapter, IndexRowType indexRowType)
    {
        this.keyCreator = adapter;
        this.keyState = adapter.createKey();
        resetForWrite(indexRowType.index(), keyState);
        this.indexRowType = indexRowType;
        //this.leafmostTable = index.leafMostTable();
        //this.hKeyCache = new HKeyCache<>(adapter);
        //this.types = index.types();
    }
    
    private Index index;
    private Key keyState;
    private Value value;
    private final IndexRowType indexRowType;

    private int pKeyFields;
    private int pKeyAppends = 0;

    
    //protected final HKeyCache<PersistitHKey> hKeyCache;
    //protected final Table leafmostTable;
    //private final TInstance[] types;
    private final KeyCreator keyCreator;


}
