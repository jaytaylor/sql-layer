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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.*;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import static java.lang.Math.min;

public abstract class PersistitIndexRow extends AbstractRow
{
    // Object interface

    @Override
    public final String toString()
    {
        ValueTarget buffer = AkibanAppender.of(new StringBuilder()).asValueTarget();
        buffer.putString("(");
        for (int i = 0; i < indexRowType.nFields(); i++) {
            if (i > 0) {
                buffer.putString(", ");
            }
            Converters.convert(eval(i), buffer);
        }
        buffer.putString(")->");
        buffer.putString(hKey().toString());
        return buffer.toString();
    }
    
    // BoundExpressions interface

    /**
     * Compares two rows and indicates if and where they differ.
     * @param row The row to be compared to this row.
     * @param leftStartIndex First field to compare in this row.
     * @param rightStartIndex First field to compare in the other row.
     * @param fieldCount Number of fields to compare.
     * @return 0 if all fields are equal. A negative value indicates that this row ranks lower than the other row.
     * A positive value indicates that the other row ranks lower. In both non-zero cases, the absolute value
     * of the return value is the position of the field that differed, starting the numbering at 1.
     * E.g. a return value of -2 means that the first fields of the rows match, and that in the second field,
     * this row had the smaller value.
     */
    @Override
    public final int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        PersistitIndexRow that = (PersistitIndexRow) row;
        int thisPosition = this.indexRow.indexTo(leftStartIndex).getIndex();
        int thatPosition = that.indexRow.indexTo(rightStartIndex).getIndex();
        int thisEnd = this.indexRow.indexTo(leftStartIndex + fieldCount).getIndex();
        int thatEnd = that.indexRow.indexTo(rightStartIndex + fieldCount).getIndex();
        byte[] thisBytes = this.indexRow.getEncodedBytes();
        byte[] thatBytes = that.indexRow.getEncodedBytes();
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


    // RowBase interface

    @Override
    public HKey hKey()
    {
        return hKeyCache.hKey(leafmostTable);
    }

    @Override
    public final RowType rowType()
    {
        return indexRowType;
    }

    @Override
    public final ValueSource eval(int i)
    {
        PersistitKeyValueSource keySource = keySource(i);
        keySource.attach(indexRow, i, akTypes[i]);
        return keySource;
    }

    @Override
    public PValueSource pvalue(int i) {
        PUnderlying underlying = rowType().typeInstanceAt(i).typeClass().underlyingType();
        PersistitKeyPValueSource keySource = keyPSource(i, underlying);
        keySource.attach(indexRow, i, underlying);
        return keySource;
    }

    // PersistitIndexRow interface

    public abstract IndexToHKey indexToHKey();

    public final boolean keyEmpty()
    {
        return indexRow.getEncodedSize() == 0;
    }

    public long tableBitmap()
    {
        throw new UnsupportedOperationException(getClass().toString());
    }

    public static PersistitIndexRow tableIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
        throws PersistitException
    {
        return new PersistitTableIndexRow(adapter, indexRowType);
    }

    public static PersistitIndexRow groupIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
        throws PersistitException
    {
        return new PersistitGroupIndexRow(adapter, indexRowType);
    }

    // For use by this package

    void copyFromExchange(Exchange exchange) throws PersistitException
    {
        PersistitHKey leafmostHKey = hKeyCache.hKey(leafmostTable);
        exchange.getKey().copyTo(indexRow);
        adapter.persistit().constructHKeyFromIndexKey(leafmostHKey.key(), indexRow, indexToHKey());
    }

    // For use by subclasses

    protected PersistitIndexRow(PersistitAdapter adapter, IndexRowType indexRowType) throws PersistitException
    {
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        assert indexRowType.nFields() == indexRowType.index().getAllColumns().size();
        this.akTypes = new AkType[indexRowType.nFields()];
        for (IndexColumn indexColumn : indexRowType.index().getAllColumns()) {
            this.akTypes[indexColumn.getPosition()] = indexColumn.getColumn().getType().akType();
        }
        this.indexRow = adapter.persistit().getKey(adapter.getSession());
        this.leafmostTable = (UserTable) indexRowType.index().leafMostTable();
        this.hKeyCache = new HKeyCache<PersistitHKey>(adapter);
    }

    // For use by this class

    private PersistitKeyValueSource keySource(int i)
    {
        if (keySources == null)
            keySources = new PersistitKeyValueSource[indexRowType.nFields()];
        if (keySources[i] == null) {
            keySources[i] = new PersistitKeyValueSource();
        }
        return keySources[i];
    }

    private PersistitKeyPValueSource keyPSource(int i, PUnderlying underlying)
    {
        if (keyPSources == null)
            keyPSources = new PersistitKeyPValueSource[indexRowType.nFields()];
        if (keyPSources[i] == null) {
            keyPSources[i] = new PersistitKeyPValueSource(underlying);
        }
        return keyPSources[i];
    }



    // Object state

    protected final PersistitAdapter adapter;
    protected final IndexRowType indexRowType;
    protected AkType[] akTypes;
    protected PersistitKeyValueSource[] keySources;
    protected PersistitKeyPValueSource[] keyPSources;
    protected final Key indexRow;
    protected final HKeyCache<PersistitHKey> hKeyCache;
    protected final UserTable leafmostTable;

    private static class PersistitTableIndexRow extends PersistitIndexRow
    {
        // RowBase interface
        
        @Override
        public HKey ancestorHKey(UserTable table)
        {
            PersistitHKey ancestorHKey;
            PersistitHKey leafmostHKey = hKeyCache.hKey(leafmostTable);
            if (table == leafmostTable) {
                ancestorHKey = leafmostHKey;
            } else {
                ancestorHKey = hKeyCache.hKey(table);
                leafmostHKey.copyTo(ancestorHKey);
                ancestorHKey.useSegments(table.getDepth() + 1);
            }
            return ancestorHKey;
        }

        // PersistitTableIndexRow interface

        @Override
        public IndexToHKey indexToHKey()
        {
            return index.indexToHKey();
        }

        public PersistitTableIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
            throws PersistitException
        {
            super(adapter, indexRowType);
            this.index = (TableIndex) indexRowType.index();
        }

        private final TableIndex index;
    }

    private static class PersistitGroupIndexRow extends PersistitIndexRow
    {
        // RowBase interface

        @Override
        public HKey ancestorHKey(UserTable table)
        {
            PersistitHKey ancestorHKey = hKeyCache.hKey(table);
            adapter.persistit().constructHKeyFromIndexKey(ancestorHKey.key(),
                                                          indexRow,
                                                          index.indexToHKey(table.getDepth()));
            return ancestorHKey;
        }

        // PersistitGroupIndexRow interface

        public IndexToHKey indexToHKey()
        {
            return index.indexToHKey(index.leafMostTable().getDepth());
        }

        public long tableBitmap()
        {
            return tableBitmap;
        }

        public PersistitGroupIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
            throws PersistitException
        {
            super(adapter, indexRowType);
            this.index = (GroupIndex) indexRowType.index();
        }

        // For use by this package

        void copyFromExchange(Exchange exchange) throws PersistitException
        {
            super.copyFromExchange(exchange);
            tableBitmap = exchange.getValue().getLong();
        }

        private final GroupIndex index;
        private long tableBitmap;
    }
}
