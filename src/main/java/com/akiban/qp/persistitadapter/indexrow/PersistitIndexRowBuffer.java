/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.persistitadapter.indexrow;

import com.akiban.ais.model.*;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.indexcursor.OldExpressionsSortKeyAdapter;
import com.akiban.qp.persistitadapter.indexcursor.PValueSortKeyAdapter;
import com.akiban.qp.persistitadapter.indexcursor.SortKeyAdapter;
import com.akiban.qp.persistitadapter.indexcursor.SortKeyTarget;
import com.akiban.qp.row.IndexRow;
import com.akiban.qp.util.PersistitKey;
import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.geophile.Space;
import com.akiban.server.geophile.SpaceLatLon;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataPValueSource;
import com.akiban.server.rowdata.RowDataSource;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.util.ArgumentValidation;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

import static java.lang.Math.min;

/*
 * 
 * Index row formats:
 * 
 * NON-UNIQUE INDEX:
 * 
 * - Persistit key contains all declared and undeclared (hkey) fields.
 * 
 * PRIMARY KEY INDEX:
 * 
 * - Persistit key contains all declared fields
 * 
 * - Persistit value contains all undeclared fields.
 * 
 * UNIQUE INDEX:
 * 
 * - Persistit key contains all declared fields
 * 
 * - Persistit key also contains one more long field, needed to ensure that insertion of an index row that contains
 *   at least one null and matches a row already in the index (including any nulls) is not considered a duplicate.
 *   For an index row with no nulls, this field contains zero. For a field with nulls, this field contains a value
 *   that is unique within the index. This mechanism is not needed for primary keys because primary keys can only
 *   contain NOT NULL columns.
 * 
 * - Persistit value contains all undeclared fields.
 * 
 */

public class PersistitIndexRowBuffer extends IndexRow implements Comparable<PersistitIndexRowBuffer>
{
    // Comparable interface

    @Override
    public int compareTo(PersistitIndexRowBuffer that)
    {
        return compareTo(that, null);
    }

    // BoundExpressions interface

    public final int compareTo(BoundExpressions row, int thisStartIndex, int thatStartIndex, int fieldCount)
    {
        // The dependence on field positions and fieldCount is a problem for spatial indexes
        if (index.isSpatial()) {
            throw new UnsupportedOperationException(index.toString());
        }
        // field and byte indexing is as if the pKey and pValue were one contiguous array of bytes. But we switch
        // from pKey to pValue as needed to avoid having to actually copy the bytes into such an array.
        PersistitIndexRowBuffer that = (PersistitIndexRowBuffer) row;
        Key thisKey;
        Key thatKey;
        if (thisStartIndex < this.pKeyFields) {
            thisKey = this.pKey;
        } else {
            thisKey = this.pValue;
            thisStartIndex -= this.pKeyFields;
        }
        if (thatStartIndex < that.pKeyFields) {
            thatKey = that.pKey;
        } else {
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
                    thisBytes = this.pValue.getEncodedBytes();
                    thisPosition = 0;
                }
                if (thatStartIndex + eqSegments == that.pKeyFields) {
                    thatBytes = that.pValue.getEncodedBytes();
                    thatPosition = 0;
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
    public void initialize(RowData rowData, Key hKey)
    {
        pKeyAppends = 0;
        int indexField = 0;
        if (spatialHandler != null) {
            spatialHandler.bind(rowData);
            pKey().append(spatialHandler.zValue());
            indexField = spatialHandler.dimensions();
        }
        IndexRowComposition indexRowComp = index.indexRowComposition();
        FieldDef[] fieldDefs = index.indexDef().getRowDef().getFieldDefs();
        RowDataSource rowDataValueSource = Types3Switch.ON ? new RowDataPValueSource() : new RowDataValueSource();
        while (indexField < indexRowComp.getLength()) {
            if (indexRowComp.isInRowData(indexField)) {
                FieldDef fieldDef = fieldDefs[indexRowComp.getFieldPosition(indexField)];
                Column column = fieldDef.column();
                rowDataValueSource.bind(fieldDef, rowData);
                pKeyTarget().append(rowDataValueSource, column.getType().akType(), column.tInstance(), column.getCollator());
            } else if (indexRowComp.isInHKey(indexField)) {
                PersistitKey.appendFieldFromKey(pKey(), hKey, indexRowComp.getHKeyPosition(indexField));
            } else {
                throw new IllegalStateException("Invalid IndexRowComposition: " + indexRowComp);
            }
            indexField++;
            pKeyAppends++;
        }
    }

    @Override
    public <S> void append(S source, AkType type, TInstance tInstance, AkCollator collator)
    {
        pKeyTarget().append(source, type, tInstance, collator);
        pKeyAppends++;
    }

    @Override
    public void close(boolean forInsert)
    {
        // Write null-separating value if necessary
        if (index.isUniqueAndMayContainNulls()) {
            long nullSeparator = 0;
            if (forInsert) {
                boolean hasNull = false;
                int keyFields = index.getKeyColumns().size();
                for (int f = 0; !hasNull && f < keyFields; f++) {
                    pKey.indexTo(f);
                    hasNull = pKey.isNull();
                }
                if (hasNull) {
                    nullSeparator = index.nextNullSeparatorValue(adapter.persistit().treeService());
                }
            }
            // else: We're creating an index row to update or delete. Don't need a new null separator value.
            pKey.append(nullSeparator);
        }
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
            PersistitKey.appendFieldFromKey(target, pKey, position);
        } else {
            PersistitKey.appendFieldFromKey(target, pValue, position - pKeyFields);
        }
        pKeyAppends++;
    }

    public void append(Key.EdgeValue edgeValue)
    {
        // An edgeValue is only useful when attached to pKey, not to pValue. This should only happen when
        // we've written the last part of the key. DON'T increment pKeyAppends since it isn't a real
        // key segment being appended.
        if (pKeyAppends <= pKeyFields) {
            pKey.append(edgeValue);
        }
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

    public PersistitIndexRowBuffer(PersistitAdapter adapter)
    {
        ArgumentValidation.notNull("adapter", adapter);
        this.adapter = adapter;
    }

    public boolean keyEmpty()
    {
        return pKey.getEncodedSize() == 0;
    }

    public int compareTo(PersistitIndexRowBuffer that, boolean[] ascending)
    {
        int c;
        byte[] thisBytes = this.pKey.getEncodedBytes();
        byte[] thatBytes = that.pKey.getEncodedBytes();
        int b = 0; // byte position
        int f = 0; // field position
        int end = min(this.pKey.getEncodedSize(), that.pKey.getEncodedSize());
        while (b < end) {
            int thisByte = thisBytes[b] & 0xff;
            int thatByte = thatBytes[b] & 0xff;
            c = thisByte - thatByte;
            if (c != 0) {
                return ascending == null || ascending[f] ? c : -c;
            } else {
                b++;
                if (thisByte == 0) {
                    f++;
                }
            }
        }
        // Compare pValues, if there are any
        thisBytes = this.pValue == null ? null : this.pValue.getEncodedBytes();
        thatBytes = that.pValue == null ? null : that.pValue.getEncodedBytes();
        if (thisBytes == null && thatBytes == null) {
            return 0;
        } else if (thisBytes == null) {
            return ascending == null || ascending[f] ? -1 : 1;
        } else if (thatBytes == null) {
            return ascending == null || ascending[f] ? 1 : -1;
        }
        b = 0;
        end = min(this.pValue.getEncodedSize(), that.pValue.getEncodedSize());
        while (b < end) {
            int thisByte = thisBytes[b] & 0xff;
            int thatByte = thatBytes[b] & 0xff;
            c = thisByte - thatByte;
            if (c != 0) {
                return ascending == null || ascending[f] ? c : -c;
            } else {
                b++;
                if (thisByte == 0) {
                    f++;
                }
            }
        }
        return 0;
    }

    // For testing only. It does an allocation per call, and is not appropriate for product use.
    public long nullSeparator()
    {
        PersistitKeyValueSource valueSource = new PersistitKeyValueSource();
        valueSource.attach(pKey, pKeyFields, AkType.LONG, null);
        return valueSource.getLong();
    }

    // For use by subclasses

    protected void attach(PersistitKeyValueSource source, int position, AkType type, AkCollator collator)
    {
        if (position < pKeyFields) {
            source.attach(pKey, position, type, collator);
        } else {
            source.attach(pValue, position - pKeyFields, type, collator);
        }
    }

    protected void attach(PersistitKeyPValueSource source, int position, PUnderlying type)
    {
        if (index.isSpatial()) {
            throw new UnsupportedOperationException("Spatial indexes don't implement types3 yet");
        }
        if (position < pKeyFields) {
            source.attach(pKey, position, type);
        } else {
            source.attach(pValue, position - pKeyFields, type);
        }
    }

    protected void copyFrom(Exchange exchange) throws PersistitException
    {
        exchange.getKey().copyTo(pKey);
        if (index.isUnique()) {
            byte[] source = exchange.getValue().getByteArray();
            pValue.setEncodedSize(source.length);
            byte[] target = pValue.getEncodedBytes();
            System.arraycopy(source, 0, target, 0, source.length);
        }
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
                    // A spatial index has a single key column (the z-value), representing the declared key columns.
                    indexField = indexField - index.getKeyColumns().size() + 1;
                }
                Key keySource;
                if (indexField < pKeyFields) {
                    keySource = pKey;
                } else {
                    keySource = pValue;
                    indexField -= pKeyFields;
                }
                if (indexField < 0 || indexField > keySource.getDepth()) {
                    throw new IllegalStateException(String.format("keySource: %s, indexField: %s",
                                                                  keySource, indexField));
                }
                PersistitKey.appendFieldFromKey(hKey, keySource, indexField);
            }
        }
    }

    // TODO: For pooling experiment
    public void reset()
    {
        pKey.clear();
        if (pValue != null) {
            pValue.clear();
        }
    }
    // TODO: End of pooling experiment

    // For use by this class

    private <S> SortKeyTarget<S> pKeyTarget()
    {
        return pKeyAppends < pKeyFields ? pKeyTarget : pValueTarget;
    }

    private Key pKey()
    {
        return pKeyAppends < pKeyFields ? pKey : pValue;
    }

    private void reset(Index index, Key key, Value value, boolean writable)
    {
        // TODO: Lots of this, especially allocations, should be moved to the constructor.
        // TODO: Or at least not repeated on reset.
        assert !index.isUnique() || index.isTableIndex() : index;
        this.index = index;
        this.pKey = key;
        if (this.pValue == null) {
            this.pValue = adapter.newKey();
        } else {
            this.pValue.clear();
        }
        this.value = value;
        if (index.isSpatial()) {
            this.nIndexFields = index.getAllColumns().size() - index.getKeyColumns().size() + 1;
            this.pKeyFields = this.nIndexFields;
            if (this.spatialHandler == null) {
                this.spatialHandler = new SpatialHandler();
            }
        } else {
            this.nIndexFields = index.getAllColumns().size();
            this.pKeyFields = index.isUnique() ? index.getKeyColumns().size() : index.getAllColumns().size();
            this.spatialHandler = null;
        }
        if (writable) {
            if (this.pKeyTarget == null) {
                this.pKeyTarget = SORT_KEY_ADAPTER.createTarget();
            }
            this.pKeyTarget.attach(key);
            this.pKeyAppends = 0;
            if (index.isUnique()) {
                if (this.pValueTarget == null) {
                    this.pValueTarget = SORT_KEY_ADAPTER.createTarget();
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
                value.getByteArray(pValue.getEncodedBytes(), 0, 0, value.getArrayLength());
                pValue.setEncodedSize(value.getArrayLength());
            }
            this.pKeyTarget = null;
            this.pValueTarget = null;
        }
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
    // The mapping: For a non-unique index, all of an index's columns (declared and undeclared) are stored in
    // the Persistit Key. For a unique index, the declared columns are stored in the Persistit Key while the
    // remaining columns are stored in the Persistit Value. Group indexes are never unique, so all columns
    // are in the Persistit Key and the Persistit Value is used to store the "table bitmap".
    //
    // Terminology: To try and avoid confusion, the terms pKey and pValue will be used when referring to Persistit
    // Keys and Values. The term key will refer to an index key.
    //
    // So why is pValueAppender a PersistitKeyAppender? Because it is convenient to treat index fields
    // in the style of Persistit Key fields. That permits, for example, byte[] comparisons to determine how values
    // that happen to reside in a Persistit Value (i.e., an undeclared field of an index row for a unique index).
    // So as an index row is being created, we deal entirely with Persisitit Keys, via pKeyAppender or pValueAppender.
    // Only when it is time to write the row are the bytes managed by the pValueAppender written as a single
    // Persistit Value.
    protected final PersistitAdapter adapter;
    protected Index index;
    protected int nIndexFields;
    private Key pKey;
    private Key pValue;
    private SortKeyTarget pKeyTarget;
    private SortKeyTarget pValueTarget;
    private int pKeyFields;
    private Value value;
    private int pKeyAppends = 0;
    private SpatialHandler spatialHandler;
    private final SortKeyAdapter SORT_KEY_ADAPTER =
            Types3Switch.ON
                    ? PValueSortKeyAdapter.INSTANCE
                    : OldExpressionsSortKeyAdapter.INSTANCE;

    // Inner classes

    // TODO: types3 version
    private class SpatialHandler
    {
        public int dimensions()
        {
            return dimensions;
        }

        public void bind(RowData rowData)
        {
            for (int d = 0; d < dimensions; d++) {
                rowDataValueSource.bind(fieldDefs[d], rowData);
                switch (types[d]) {
                    case INT:
                        coords[d] = rowDataValueSource.getInt();
                        break;
                    case LONG:
                        coords[d] = rowDataValueSource.getLong();
                        break;
                    case DECIMAL:
                        coords[d] =
                            d == 0
                            ? SpaceLatLon.scaleLat(rowDataValueSource.getDecimal())
                            : SpaceLatLon.scaleLon(rowDataValueSource.getDecimal());
                        break;
                    default:
                        assert false : fieldDefs[d].column();
                        break;
                }
            }
        }

        public long zValue()
        {
            return space.shuffle(coords);
        }

        private Space space;
        private final int dimensions;
        private final AkType[] types;
        private final FieldDef[] fieldDefs;
        private final long[] coords;
        private final RowDataValueSource rowDataValueSource;

        {
            space = ((TableIndex)index).space();
            dimensions = space.dimensions();
            assert index.getKeyColumns().size() == dimensions;
            types = new AkType[dimensions];
            fieldDefs = new FieldDef[dimensions];
            coords = new long[dimensions];
            rowDataValueSource = new RowDataValueSource();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                int position = indexColumn.getPosition();
                Column column = indexColumn.getColumn();
                types[position] = column.getType().akType();
                fieldDefs[position] = column.getFieldDef();
            }
        }
    }
}
