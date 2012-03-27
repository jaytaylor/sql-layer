/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.AkibanAppender;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import static java.lang.Math.min;

public class PersistitIndexRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
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
        buffer.putString(hKey.toString());
        return buffer.toString();
    }
    
    // BoundExpressions interface

    /**
     * Compares two rows and indicates if and where they differ.
     * @param row The row to be compared to this row.
     * @param leftStartIndex First field to compare in this row.
     * @param rightStartIndex First field to compare in the other row.
     * @param fieldCount Number of fields to compare.
     * @return 0 if all fields are equal. A negative value indicates that this row had the first field
     * that was not equal to the corresponding field in the other row. A positive value indicates that the
     * other row had the first field that was not equal to the corresponding field in this row. In both non-zero
     * cases, the absolute value of the return value is the position of the field that differed, starting the numbering
     * at 1. E.g. a return value of -2 means that the first fields of the rows match, and that in the second field,
     * this row had the smaller value.
     */
    @Override
    public int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
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
        if (c == 0) {
            c = thisByteCount - thatByteCount;
        }
        if (c < 0) {
            c = -(eqSegments + 1);
        } else if (c > 0) {
            c = eqSegments + 1;
        }
        return c;
    }


    // RowBase interface

    @Override
    public RowType rowType()
    {
        return indexRowType;
    }

    @Override
    public ValueSource eval(int i) 
    {
        IndexColumn column = indexColumns[i];
        PersistitKeyValueSource keySource = keySource(i);
        keySource.attach(indexRow, column);
        return keySource;
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    // PersistitIndexRow interface

    public PersistitIndexRow(PersistitAdapter adapter, IndexRowType indexRowType) throws PersistitException
    {
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        this.indexColumns = new IndexColumn[index().getAllColumns().size()];
        index().getAllColumns().toArray(this.indexColumns);
        this.keySources = new PersistitKeyValueSource[indexRowType.nFields()];
        this.indexRow = adapter.persistit().getKey(adapter.session());
        this.hKey = new PersistitHKey(adapter, index().hKey());
    }

    // For use by this package

    void copyFromExchange(Exchange exchange)
    {
        // Extract the hKey from the exchange, using indexRow as a convenient Key to bridge Exchange
        // and PersistitHKey.
        adapter.persistit().constructHKeyFromIndexKey(indexRow, exchange.getKey(), index());
        hKey.copyFrom(indexRow);
        // Now copy the entire index record into indexRow.
        exchange.getKey().copyTo(indexRow);
    }

    // For use by this class

    private Index index()
    {
        return indexRowType.index();
    }

    private PersistitKeyValueSource keySource(int i)
    {
        if (keySources[i] == null) {
            keySources[i] = new PersistitKeyValueSource();
        }
        return keySources[i];
    }
    
    // Object state

    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private IndexColumn[] indexColumns;
    private final PersistitKeyValueSource[] keySources;
    private final Key indexRow;
    private PersistitHKey hKey;
}
