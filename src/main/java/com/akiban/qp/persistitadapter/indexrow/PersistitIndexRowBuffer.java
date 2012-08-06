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
import com.akiban.qp.row.IndexRow;
import com.akiban.qp.util.PersistitKey;
import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.geophile.Space;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.store.PersistitKeyAppender;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

import static java.lang.Math.min;

public class PersistitIndexRowBuffer extends IndexRow
{
    // BoundExpressions interface

    public final int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        // The dependence on field positions and fieldCount is a problem for spatial indexes
        if (index.isSpatial()) {
            throw new UnsupportedOperationException(index.toString());
        }
        PersistitIndexRowBuffer that = (PersistitIndexRowBuffer) row;
        Key thisKey = this.keyAppender.key();
        Key thatKey = that.keyAppender.key();
        int thisPosition = thisKey.indexTo(leftStartIndex).getIndex();
        int thatPosition = thatKey.indexTo(rightStartIndex).getIndex();
        int thisEnd = thisKey.indexTo(leftStartIndex + fieldCount).getIndex();
        int thatEnd = thatKey.indexTo(rightStartIndex + fieldCount).getIndex();
        byte[] thisBytes = thisKey.getEncodedBytes();
        byte[] thatBytes = thatKey.getEncodedBytes();
        int thisByteCount = thisEnd - thisPosition;
        int thatByteCount = thatEnd - thatPosition;
        int thisStop = thisPosition + min(thisByteCount, thatByteCount);
        int c = 0;
        int eqSegments = 0;
        while (thisPosition < thisStop) {
            byte thisByte = thisBytes[thisPosition++];
            byte thatByte = thatBytes[thatPosition++];
            c = thisByte - thatByte;
            if (c != 0) {
                break;
            } else if (thisByte == 0) {
                // thisByte = thatByte = 0
                eqSegments++;
            }
        }
        // If c == 0 then thisPosition == thisStop and the two subarrays must match.
        if (c < 0) {
            c = -(eqSegments + 1);
        } else if (c > 0) {
            c = eqSegments + 1;
        }
        return c;
    }

    // IndexRow interface

    public void append(Column column, ValueSource source)
    {
        // There is no hard requirement that the index is a group index. But while we're adding support for
        // spatial, we just want to be precise about what kind of index is in use.
        assert index.isGroupIndex();
        keyAppender.append(source, column);
    }

    public void initialize(RowData rowData, Key hKey)
    {
        int indexField = 0;
        if (spatialHandler != null) {
            spatialHandler.bind(rowData);
            keyAppender.append(spatialHandler.zValue());
            indexField = spatialHandler.dimensions();
        }
        IndexRowComposition indexRowComp = index.indexRowComposition();
        FieldDef[] fieldDefs = index.indexDef().getRowDef().getFieldDefs();
        while (indexField < indexRowComp.getLength()) {
            if (indexRowComp.isInRowData(indexField)) {
                keyAppender.append(fieldDefs[indexRowComp.getFieldPosition(indexField)], rowData);
            } else if (indexRowComp.isInHKey(indexField)) {
                keyAppender.appendFieldFromKey(hKey, indexRowComp.getHKeyPosition(indexField));
            } else {
                throw new IllegalStateException("Invalid IndexRowComposition: " + indexRowComp);
            }
            indexField++;
        }
    }

    public boolean keyEmpty()
    {
        return keyAppender.key().getEncodedSize() == 0;
    }

    // PersistitIndexRowBuffer interface

    public void tableBitmap(long bitmap)
    {
        value.put(bitmap);
    }

    public PersistitIndexRowBuffer()
    {}

    // For table index rows
    public void reset(Index index, Key key)
    {
        this.index = index;
        key.clear();
        this.keyAppender = PersistitKeyAppender.create(key);
        this.value = null;
        if (index.isSpatial()) {
            this.spatialHandler = new SpatialHandler();
            this.nIndexFields = index.getAllColumns().size() - index.getKeyColumns().size() + 1;
        } else {
            this.spatialHandler = null;
            this.nIndexFields = index.getAllColumns().size();
        }
    }

    // For group index rows
    public void reset(Index index, Key key, Value value)
    {
        reset(index, key);
        value.clear();
        this.value = value;
    }

    // For use by subclasses

    protected void attach(PersistitKeyValueSource source, int position, AkType type, AkCollator collator)
    {
        source.attach(keyAppender.key(), position, type, collator);
    }

    protected void attach(PersistitKeyPValueSource source, int position, PUnderlying type)
    {
        if (index.isSpatial()) {
            throw new UnsupportedOperationException("Spatial indexes don't implement types3 yet");
        }
        source.attach(keyAppender.key(), position, type);
    }

    protected void copyFrom(Exchange exchange) throws PersistitException
    {
        exchange.getKey().copyTo(keyAppender.key());
    }

    protected void constructHKeyFromIndexKey(Key hKey, IndexToHKey indexToHKey)
    {
        Key indexRowKey = keyAppender.key();
        hKey.clear();
        for (int i = 0; i < indexToHKey.getLength(); i++) {
            if (indexToHKey.isOrdinal(i)) {
                hKey.append(indexToHKey.getOrdinal(i));
            } else {
                int depth = indexToHKey.getIndexRowPosition(i);
                if (index.isSpatial()) {
                    // A spatial index has a single key column (the z-value), representing the declared key columns.
                    depth = depth - index.getKeyColumns().size() + 1;
                }
                if (depth < 0 || depth > indexRowKey.getDepth()) {
                    throw new IllegalStateException(
                        "IndexKey too shallow - requires depth=" + depth
                        + ": " + indexRowKey);
                }
                PersistitKey.appendFieldFromKey(hKey, indexRowKey, depth);
            }
        }
    }

    // Object state

    protected Index index;
    protected int nIndexFields;
    private PersistitKeyAppender keyAppender;
    private Value value;
    private SpatialHandler spatialHandler;

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
                    // TODO: DECIMAL
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
