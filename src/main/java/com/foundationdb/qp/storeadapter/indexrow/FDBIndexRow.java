/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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
package com.foundationdb.qp.storeadapter.indexrow;

import static java.lang.Math.min;

import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexToHKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.storeadapter.indexcursor.SortKeyAdapter;
import com.foundationdb.qp.storeadapter.indexcursor.SortKeyTarget;
import com.foundationdb.qp.storeadapter.indexcursor.ValueSortKeyAdapter;
import com.foundationdb.qp.util.PersistitKey;
import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
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
    protected ValueSource uncheckedValue(int i) {
        PersistitKeyValueSource source = new PersistitKeyValueSource(types[i]);
        source.attach(iKey, i, types[i]);
        return source;
    }

    @Override
    public HKey hKey()
    {
        assert this.leafTableHKey != null;
        return this.leafTableHKey;
    }
    
    @Override
    public HKey ancestorHKey(Table table)
    {
        PersistitHKey ancestorHKey; 
        
        IndexToHKey indexToHKey;
        if (index.isGroupIndex()) {
            ancestorHKey = new PersistitHKey(keyCreator.createKey(), table.hKey());
            indexToHKey = ((GroupIndex)index).indexToHKey(table.getDepth());
        } else {
            ancestorHKey = new PersistitHKey(keyCreator.createKey(), index.leafMostTable().hKey());
            indexToHKey = ((TableIndex)index).indexToHKey();
        }
        Key hKey = ancestorHKey.key();
        hKey.clear();
        for (int i = 0; i < indexToHKey.getLength(); i++) {
            if (indexToHKey.isOrdinal(i)) {
                hKey.append(indexToHKey.getOrdinal(i));
            } else {
                int indexField = indexToHKey.getIndexRowPosition(i);
                if (index.isSpatial()) {
                    // A spatial index has a single key column (the z-value), representing the declared spatial key columns.
                    if (indexField > index.firstSpatialArgument())
                        indexField -= index.dimensions() - 1;
                }
                Key keySource = iKey;
                if (indexField < 0 || indexField > keySource.getDepth()) {
                    throw new IllegalStateException(String.format("keySource: %s, indexField: %s",
                                                                  keySource, indexField));
                }
                PersistitKey.appendFieldFromKey(hKey, keySource, indexField, index.getIndexName());
            }
        }
        if (index.isTableIndex() && index.leafMostTable() != table) {
            this.leafTableHKey = ancestorHKey; 
            ancestorHKey = new PersistitHKey(keyCreator.createKey(), table.hKey());
            this.leafTableHKey.copyTo(ancestorHKey);
            ancestorHKey.useSegments(table.getDepth() + 1);
        }
        
        return ancestorHKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S> void append(S source, TInstance type) {
        pKeyTarget.append(source, type);
    }

    @Override
    public void append(EdgeValue value) {
        if (value == EdgeValue.BEFORE)
           iKey.append(Key.BEFORE);
        else if (value == EdgeValue.AFTER)
            iKey.append(Key.AFTER);
        else
            throw new UnsupportedOperationException(value.toString());

    }

    @Override
    public void resetForRead(Index index, Key key, Value value) {
        this.index = index;
        this.iKey = key;
        this.iValue = value;
        this.pKeyTarget = null;
        this.pKeyFields = index.getAllColumns().size();
        if (value != null) {
            value.getByteArray(iValue.getEncodedBytes(), 0, 0, value.getArrayLength());
            iValue.setEncodedSize(value.getArrayLength());
        }
    }

    @Override
    public void resetForWrite (Index index, Key key) {
        this.index = index;
        this.iKey = key;
        this.iValue = null;
        this.pKeyFields = index.getAllColumns().size();
        if (this.pKeyTarget == null) {
            this.pKeyTarget = SORT_KEY_ADAPTER.createTarget(index.getIndexName());
        }
        this.pKeyTarget.attach(key);
        
    }
    
    @Override 
    public int compareTo(IndexRow thatKey, int startBoundColumns) {
        return compareTo (thatKey, startBoundColumns, null);
    }

    @Override
    public int compareTo(IndexRow thatKey, int fieldCount, boolean[] ascending) {
        FDBIndexRow that = (FDBIndexRow)thatKey;
        
        if(fieldCount <= 0) {
            return 0;
        }
        int c;
        byte[] thisBytes = this.iKey.getEncodedBytes();
        byte[] thatBytes = that.iKey.getEncodedBytes();
        int b = 0; // byte position
        int f = 0; // field position
        int thisEnd = this.iKey.getEncodedSize();
        int thatEnd = that.iKey.getEncodedSize();
        int end = min(thisEnd, thatEnd);
        while (b < end && f < pKeyFields) {
            int thisByte = thisBytes[b] & 0xff;
            int thatByte = thatBytes[b] & 0xff;
            c = thisByte - thatByte;
            if (c != 0) {
                return isSet(ascending, f, c, -c);
            } else {
                b++;
                if (thisByte == 0) {
                    if(++f == fieldCount) {
                        return 0;
                    }
                }
            }
        }
        if (thisEnd > thatEnd) {
            return isSet(ascending, f, 1, -1);
        }
        if (thatEnd > thisEnd) {
            return isSet(ascending, f, -1, 1);
        }
        // Compare pValues, if there are any
        thisBytes = this.iValue == null ? null : this.iValue.getEncodedBytes();
        thatBytes = that.iValue == null ? null : that.iValue.getEncodedBytes();
        if (thisBytes == null && thatBytes == null) {
            return 0;
        } else if (thisBytes == null) {
            return isSet(ascending, f, -1, 1);
        } else if (thatBytes == null) {
            return isSet(ascending, f, 1, -1);
        }
        b = 0;
        thisEnd = this.iValue.getEncodedSize();
        thatEnd = that.iValue.getEncodedSize();
        end = min(thisEnd, thatEnd);
        while (b < end) {
            int thisByte = thisBytes[b] & 0xff;
            int thatByte = thatBytes[b] & 0xff;
            c = thisByte - thatByte;
            if (c != 0) {
                return isSet(ascending, f, c, -c);
            } else {
                b++;
                if (thisByte == 0) {
                    if(++f == fieldCount) {
                        return 0;
                    }
                }
            }
        }
        if (thisEnd > thatEnd) {
            return isSet(ascending, f, 1, -1);
        }
        if (thatEnd > thisEnd) {
            return isSet(ascending, f,  -1, 1);
        }
        return 0;
    }

    private static int isSet(boolean[] values, int idx, int tVal, int fVal) {
        return ((values == null) || values[idx]) ? tVal : fVal;
    }


    @Override
    public void reset() {
        iKey.clear();
        if (iValue != null) {
            iValue.clear();
        }
    }

    
    @Override
    public void copyPersistitKeyTo(Key key)
    {
        iKey.copyTo(key);
    }
    
    @Override
    public void appendFieldTo(int position, Key target) {
        PersistitKey.appendFieldFromKey(target, iKey, position, index.getIndexName());
    }

    @Override
    public void copyFrom(Key key, Value value) {
        key.copyTo(iKey);
        if (value != null && value.isDefined() && !value.isNull()) {
            tableBitmap = value.getLong();
        }
        leafTableHKey = ancestorHKey(index.leafMostTable());
    }

    @Override
    public boolean keyEmpty()
    {
        return iKey.getEncodedSize() == 0;
    }

    @Override
    public void tableBitmap(long bitmap) {
        tableBitmap = bitmap;
    }

    @Override
    public long tableBitmap() {
        return tableBitmap;
    }
    
    public FDBIndexRow (KeyCreator keyCreator)
    {
        ArgumentValidation.notNull("keyCreator", keyCreator);
        this.keyCreator = keyCreator;
        this.indexRowType = null;
        this.types = null;
        //this.leafmostTable = null;
    }

    public FDBIndexRow (KeyCreator adapter, IndexRowType indexRowType)
    {
        ArgumentValidation.notNull("keyCreator", adapter);
        this.keyCreator = adapter;
        this.iKey = adapter.createKey();
        this.indexRowType = indexRowType;
        this.index = indexRowType.index();
        resetForWrite(index, iKey);
        //this.leafmostTable = index.leafMostTable();
        //this.hKeyCache = new HKeyCache<>(adapter);
        this.types = index.types();
    }
    
    private Index index;
    private Key iKey;
    private Value iValue;
    private final IndexRowType indexRowType;
    private long tableBitmap;
    private SortKeyTarget pKeyTarget;

    private int pKeyFields;

    private final SortKeyAdapter<ValueSource, TPreparedExpression> SORT_KEY_ADAPTER = ValueSortKeyAdapter.INSTANCE;
    
    //protected final HKeyCache<PersistitHKey> hKeyCache;
    //protected final Table leafmostTable;
    private final TInstance[] types;
    private final KeyCreator keyCreator;
    private HKey leafTableHKey;
}
