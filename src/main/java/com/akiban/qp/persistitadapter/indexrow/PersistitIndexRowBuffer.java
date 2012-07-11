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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.IndexRow;
import com.akiban.qp.util.PersistitKey;
import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.store.PersistitKeyAppender;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

public class PersistitIndexRowBuffer extends IndexRow
{
    // BoundExpressions interface

    public final int compareTo(BoundExpressions row, int thisStartIndex, int thatStartIndex, int fieldCount)
    {
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
            c = thisByte - thatByte;
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

    public void append(FieldDef fieldDef, RowData rowData)
    {
        keyAppender().append(fieldDef, rowData);
        pKeyAppends++;
    }

    public void append(Column column, ValueSource source)
    {
        keyAppender().append(source, column);
        pKeyAppends++;
    }

    public void appendFieldFromKey(Key fromKey, int depth)
    {
        keyAppender().appendFieldFromKey(fromKey, depth);
        pKeyAppends++;
    }

    @Override
    public void close()
    {
        // If necessary, copy pValue state into value. (Check pValueAppender, because that is non-null only in
        // a writeable PIRB.)
        if (pValueAppender != null) {
            value.clear();
            value.putByteArray(pValue.getEncodedBytes(), 0, pValue.getEncodedSize());
        }
    }

    // PersistitIndexRowBuffer interface

    public void appendFieldTo(int position, PersistitKeyAppender target)
    {
        if (position < pKeyFields) {
            PersistitKey.appendFieldFromKey(target.key(), pKey, position);
        } else {
            PersistitKey.appendFieldFromKey(target.key(), pValue, position - pKeyFields);
        }
    }

    public void tableBitmap(long bitmap)
    {
        value.put(bitmap);
    }

    public static PersistitIndexRowBuffer createEmpty(PersistitAdapter adapter, Index index, Key key, Value value)
    {
        key.clear();
        if (value != null) {
            value.clear();
        }
        return new PersistitIndexRowBuffer(adapter, index, key, value, true);
    }

    public static PersistitIndexRowBuffer createInitialized(PersistitAdapter adapter, Index index, Key key, Value value)
    {
        return new PersistitIndexRowBuffer(adapter, index, key, value, false);
    }

    public boolean keyEmpty()
    {
        return pKey.getEncodedSize() == 0;
    }

    // For use by subclasses

    protected void attach(PersistitKeyValueSource source, int position, AkType type)
    {
        if (position < pKeyFields) {
            source.attach(pKey, position, type);
        } else {
            source.attach(pValue, position - pKeyFields, type);
        }
    }

    protected void attach(PersistitKeyPValueSource source, int position, PUnderlying type)
    {
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
        Key indexRowKey = pKey;
        hKey.clear();
        for (int i = 0; i < indexToHKey.getLength(); ++i) {
            if (indexToHKey.isOrdinal(i)) {
                hKey.append(indexToHKey.getOrdinal(i));
            } else {
                int indexField = indexToHKey.getIndexRowPosition(i);
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

    // For use by subclasses

    protected PersistitIndexRowBuffer(PersistitAdapter adapter, Index index, Key key, Value value, boolean writable)
    {
        assert !index.isUnique() || index.isTableIndex() : index;
        this.adapter = adapter;
        this.index = index;
        this.pKey = key;
        this.pKeyFields =
            index.isUnique()
            ? index.getKeyColumns().size()
            : index.getAllColumns().size();
        this.pValue = adapter.newKey();
        if (writable) {
            this.pKeyAppender = PersistitKeyAppender.create(key);
            this.pValueAppender =
                index.isUnique()
                ? PersistitKeyAppender.create(this.pValue)
                : null;
        } else {
            if (value != null) {
                value.getByteArray(pValue.getEncodedBytes(), 0, 0, value.getArrayLength());
                pValue.setEncodedSize(value.getArrayLength());
            }
            this.pKeyAppender = null;
            this.pValueAppender = null;
        }
        this.value = value;
    }

    // For use by this class

    private PersistitKeyAppender keyAppender()
    {
        return pKeyAppends < pKeyFields ? pKeyAppender : pValueAppender;
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
    private final Index index;
    private final Key pKey;
    private final Key pValue;
    private final PersistitKeyAppender pKeyAppender;
    private final PersistitKeyAppender pValueAppender;
    private final int pKeyFields;
    private final Value value;
    private int pKeyAppends = 0;
}
