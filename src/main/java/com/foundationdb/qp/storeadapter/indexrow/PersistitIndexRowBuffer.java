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

package com.foundationdb.qp.storeadapter.indexrow;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.storeadapter.indexcursor.ValueSortKeyAdapter;
import com.foundationdb.qp.storeadapter.indexcursor.SortKeyAdapter;
import com.foundationdb.qp.storeadapter.indexcursor.SortKeyTarget;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.util.PersistitKey;
import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.rowdata.*;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.ArgumentValidation;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;

import static java.lang.Math.min;

public class PersistitIndexRowBuffer extends IndexRow implements Comparable<PersistitIndexRowBuffer>
{
    // Comparable interface

    @Override
    public int compareTo(PersistitIndexRowBuffer that)
    {
        return compareTo(that, Integer.MAX_VALUE, null);
    }

    // Row interface

    @Override
    public ValueSource uncheckedValue(int i) {
        return null;
    }

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }

    @Override
    public final int compareTo(Row row, int thisStartIndex, int thatStartIndex, int fieldCount)
    {
        // The dependence on field positions and fieldCount is a problem for spatial indexes
        if (index.isSpatial()) {
            throw new UnsupportedOperationException(index.toString());
        }
        if (!(row instanceof PersistitIndexRowBuffer)) {
            return super.compareTo(row, thisStartIndex, thatStartIndex, fieldCount);
        }
        if (fieldCount <= 0) {
            return 0;
        }
        // field and byte indexing is as if the pKey and pValue were one contiguous array of bytes. But we switch
        // from pKey to pValue as needed to avoid having to actually copy the bytes into such an array.
        PersistitIndexRowBuffer that = (PersistitIndexRowBuffer) row;
        Key thisKey;
        Key thatKey;
        if (thisStartIndex < this.pKeyFields) {
            thisKey = this.pKey;
        } else {
            checkValueUsage();
            thisKey = this.pValue;
            thisStartIndex -= this.pKeyFields;
        }
        if (thatStartIndex < that.pKeyFields) {
            thatKey = that.pKey;
        } else {
            checkValueUsage();
            thatKey = that.pValue;
            thatStartIndex -= that.pKeyFields;
        }
        int thisPosition = thisKey.indexTo(thisStartIndex).getIndex();
        int thatPosition = thatKey.indexTo(thatStartIndex).getIndex();
        byte[] thisBytes = thisKey.getEncodedBytes();
        byte[] thatBytes = thatKey.getEncodedBytes();
        int c = 0;
        int eqSegments = 0;
        while (eqSegments < fieldCount) {
            byte thisByte = thisBytes[thisPosition++];
            byte thatByte = thatBytes[thatPosition++];
            c = (thisByte & 0xff) - (thatByte & 0xff);
            if (c != 0) {
                break;
            } else if (thisByte == 0) {
                // thisByte = thatByte = 0
                eqSegments++;
                if (thisStartIndex + eqSegments == this.pKeyFields) {
                    if (this.pValue == null) {
                        assert eqSegments == fieldCount : index;
                    } else {
                        thisBytes = this.pValue.getEncodedBytes();
                        thisPosition = 0;
                    }
                }
                if (thatStartIndex + eqSegments == that.pKeyFields) {
                    if(that.pValue == null) {
                        assert eqSegments == fieldCount : index;
                    } else {
                        thatBytes = that.pValue.getEncodedBytes();
                        thatPosition = 0;
                    }
                }
            }
        }
        // If c == 0 then the two subarrays must match.
        if (c < 0) {
            c = -(eqSegments + 1);
        } else if (c > 0) {
            c = eqSegments + 1;
        }
        return c;
    }

    // IndexRow interface

    @Override
    public void initialize(RowData rowData, Key hKey, SpatialColumnHandler spatialColumnHandler, long zValue)
    {
        pKeyAppends = 0;
        int indexField = 0;
        IndexRowComposition indexRowComp = index.indexRowComposition();
        FieldDef[] fieldDefs = index.leafMostTable().rowDef().getFieldDefs();
        RowDataSource rowDataValueSource = new RowDataValueSource();
        while (indexField < indexRowComp.getLength()) {
            // handleSpatialColumn will increment pKeyAppends once for all spatial columns
            if (spatialColumnHandler != null && spatialColumnHandler.handleSpatialColumn(this, indexField, zValue)) {
                if (indexField == index.firstSpatialArgument()) {
                    pKeyAppends++;
                }
            } else {
                if (indexRowComp.isInRowData(indexField)) {
                    FieldDef fieldDef = fieldDefs[indexRowComp.getFieldPosition(indexField)];
                    Column column = fieldDef.column();
                    rowDataValueSource.bind(fieldDef, rowData);
                    pKeyTarget().append(rowDataValueSource,
                                        column.getType());
                } else if (indexRowComp.isInHKey(indexField)) {
                    PersistitKey.appendFieldFromKey(pKey(), hKey, indexRowComp.getHKeyPosition(indexField), index
                        .getIndexName());
                } else {
                    throw new IllegalStateException("Invalid IndexRowComposition: " + indexRowComp);
                }
                pKeyAppends++;
            }
            indexField++;
        }
    }

    @Override
    public <S> void append(S source, TInstance type)
    {
        pKeyTarget().append(source, type);
        pKeyAppends++;
    }

    @Override
    public void close(Session session, Store store, boolean forInsert)
    {
        // If necessary, copy pValue state into value. (Check pValueAppender, because that is non-null only in
        // a writeable PIRB.)
        if (pValueTarget != null) {
            value.clear();
            value.putByteArray(pValue.getEncodedBytes(), 0, pValue.getEncodedSize());
        }
    }

    // PersistitIndexRowBuffer interface

    public void appendFieldTo(int position, Key target)
    {
        if (position < pKeyFields) {
            PersistitKey.appendFieldFromKey(target, pKey, position, index.getIndexName());
        } else {
            checkValueUsage();
            PersistitKey.appendFieldFromKey(target, pValue, position - pKeyFields, index.getIndexName());
        }
        pKeyAppends++;
    }

    public void append(Key.EdgeValue edgeValue)
    {
        // This is unlike other appends. An EdgeValue affects Persistit iteration, so it has to go to a Persistit
        // Key, not a Value. If we're already appending to Values (pKeyAppends >= pKeyFields), then all we can
        // do is append to the value, and then rely on beforeStart() to skip rows that don't qualify. Also,
        // don't increment pKeyAppends, because this append doesn't change where we write next. (In fact, after
        // writing an EdgeValue we shouldn't be appending more anyway.)
        pKey.append(edgeValue);
    }

    public void tableBitmap(long bitmap)
    {
        value.put(bitmap);
    }

    public void copyPersistitKeyTo(Key key)
    {
        pKey.copyTo(key);
    }

    // For table index rows
    public void resetForWrite(Index index, Key key)
    {
        reset(index, key, null, true);
    }

    // For group index rows
    public void resetForWrite(Index index, Key key, Value value)
    {
        reset(index, key, value, true);
    }

    public void resetForRead(Index index, Key key, Value value)
    {
        reset(index, key, value, false);
    }

    public PersistitIndexRowBuffer(KeyCreator keyCreator)
    {
        ArgumentValidation.notNull("keyCreator", keyCreator);
        this.keyCreator = keyCreator;
    }

    public boolean keyEmpty()
    {
        return pKey.getEncodedSize() == 0;
    }

    public int compareTo(PersistitIndexRowBuffer that, int fieldCount) {
        return compareTo(that, fieldCount, null);
    }

    public int compareTo(PersistitIndexRowBuffer that, int fieldCount, boolean[] ascending)
    {
        if(fieldCount <= 0) {
            return 0;
        }
        int c;
        byte[] thisBytes = this.pKey.getEncodedBytes();
        byte[] thatBytes = that.pKey.getEncodedBytes();
        int b = 0; // byte position
        int f = 0; // field position
        int thisEnd = this.pKey.getEncodedSize();
        int thatEnd = that.pKey.getEncodedSize();
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
        thisBytes = this.pValue == null ? null : this.pValue.getEncodedBytes();
        thatBytes = that.pValue == null ? null : that.pValue.getEncodedBytes();
        if (thisBytes == null && thatBytes == null) {
            return 0;
        } else if (thisBytes == null) {
            return isSet(ascending, f, -1, 1);
        } else if (thatBytes == null) {
            return isSet(ascending, f, 1, -1);
        }
        b = 0;
        thisEnd = this.pValue.getEncodedSize();
        thatEnd = that.pValue.getEncodedSize();
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

    // For use by subclasses

    protected void attach(PersistitKeyValueSource source, int position, TInstance type)
    {
        if (position < pKeyFields) {
            source.attach(pKey, position, type);
        } else {
            checkValueUsage();
            source.attach(pValue, position - pKeyFields, type);
        }
    }

    /** Override {@link #copyFrom(Key, Value)} if needed. */
    public final void copyFrom(Exchange ex)
    {
        copyFrom(ex.getKey(), ex.getValue());
    }

    public void copyFrom(Key key, Value value)
    {
        key.copyTo(pKey);
    }

    protected void constructHKeyFromIndexKey(Key hKey, IndexToHKey indexToHKey)
    {
        hKey.clear();
        for (int i = 0; i < indexToHKey.getLength(); i++) {
            if (indexToHKey.isOrdinal(i)) {
                hKey.append(indexToHKey.getOrdinal(i));
            } else {
                int indexField = indexToHKey.getIndexRowPosition(i);
                if (index.isSpatial()) {
                    // A spatial index has a single key column (the z-value), representing the declared spatial key columns.
                    if (indexField > index.firstSpatialArgument())
                        indexField -= index.spatialColumns() - 1;
                }
                Key keySource;
                if (indexField < pKeyFields) {
                    keySource = pKey;
                } else {
                    checkValueUsage();
                    keySource = pValue;
                    indexField -= pKeyFields;
                }
                if (indexField < 0 || indexField > keySource.getDepth()) {
                    throw new IllegalStateException(String.format("keySource: %s, indexField: %s",
                                                                  keySource, indexField));
                }
                PersistitKey.appendFieldFromKey(hKey, keySource, indexField, index.getIndexName());
            }
        }
    }

    public void reset()
    {
        pKey.clear();
        if (pValue != null) {
            pValue.clear();
        }
    }

    // For use by this class

    @SuppressWarnings("unchecked")
    private <S> SortKeyTarget<S> pKeyTarget()
    {
        if (pKeyAppends < pKeyFields) {
            return pKeyTarget;
        }
        checkValueUsage();
        return pValueTarget;
    }

    Key pKey()
    {
        if (pKeyAppends < pKeyFields) {
            return pKey;
        }
        checkValueUsage();
        return pValue;
    }

    private void reset(Index index, Key key, Value value, boolean writable)
    {
        assert !index.isUnique() || index.isTableIndex() : index;
        this.index = index;
        this.pKey = key;
        if (this.pValue == null) {
            if (ALLOCATE_VALUE_KEY) {
                this.pValue = keyCreator.createKey();
            }
        } else {
            this.pValue.clear();
        }
        this.value = value;
        if (index.isSpatial()) {
            this.nIndexFields = index.getAllColumns().size() - index.spatialColumns() + 1;
            this.pKeyFields = this.nIndexFields;
        } else {
            this.nIndexFields = index.getAllColumns().size();
            this.pKeyFields = index.getAllColumns().size();
        }
        if (writable) {
            if (this.pKeyTarget == null) {
                this.pKeyTarget = SORT_KEY_ADAPTER.createTarget(index.getIndexName());
            }
            this.pKeyTarget.attach(key);
            this.pKeyAppends = 0;
            if (index.isUnique() && ALLOCATE_VALUE_KEY) {
                if (this.pValueTarget == null) {
                    this.pValueTarget = SORT_KEY_ADAPTER.createTarget(index.getIndexName());
                }
                this.pValueTarget.attach(this.pValue);
            } else {
                this.pValueTarget = null;
            }
            if (value != null) {
                value.clear();
            }
        } else {
            if (value != null) {
                checkValueUsage();
                value.getByteArray(pValue.getEncodedBytes(), 0, 0, value.getArrayLength());
                pValue.setEncodedSize(value.getArrayLength());
            }
            this.pKeyTarget = null;
            this.pValueTarget = null;
        }
    }

    public Key getPKey() {
        return pKey;
    }

    public Key getPValue() {
        return pValue;
    }

    public Value getValue() {
        return value;
    }

    // Object state

    // The notation involving "keys" and "values" is tricky because this code deals with both the index view and
    // the persistit view, and these don't match up exactly.
    //
    // The index view of keys and values: An application-defined index has a key comprising
    // one or more columns from one table (table index) or multiple tables (group index). An index row has fields
    // corresponding to these columns, and additional fields corresponding to undeclared hkey columns.
    // Index.getKeyColumns refers to the declared columns, and Index.getAllColumns refers to the declared and
    // undeclared columns.
    //
    // The persistit view: A record managed by Persistit has a Key and a Value.
    //
    // Terminology: To try and avoid confusion, the terms pKey and pValue will be used when referring to Persistit
    // Keys and Values. The term key will refer to an index key.
    //
    // So why is pValueAppender a PersistitKeyAppender? Because it is convenient to treat index fields
    // in the style of Persistit Key fields. That permits, for example, byte[] comparisons to determine how values
    // that happen to reside in a Persistit Value.
    // So as an index row is being created, we deal entirely with Persisitit Keys, via pKeyAppender or pValueAppender.
    // Only when it is time to write the row are the bytes managed by the pValueAppender written as a single
    // Persistit Value.
    protected final KeyCreator keyCreator;
    protected Index index;
    protected int nIndexFields;
    private Key pKey;
    private SortKeyTarget pKeyTarget;
    private int pKeyFields;
    private Value value;
    private int pKeyAppends = 0;
    private final SortKeyAdapter SORT_KEY_ADAPTER = ValueSortKeyAdapter.INSTANCE;
    // Not currently instantiated, left in-case needed again
    private SortKeyTarget pValueTarget;
    private Key pValue;

    private static final boolean ALLOCATE_VALUE_KEY = false;

    private void checkValueUsage() {
        if(!ALLOCATE_VALUE_KEY) {
            throw new IllegalStateException("Unexpected fields in Value: " + index);
        }
    }

    // Inner classes

}
