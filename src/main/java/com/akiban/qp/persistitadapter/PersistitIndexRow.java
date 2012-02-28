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
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.AkibanAppender;
import com.akiban.util.SparseArray;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

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

    // RowBase interface

    @Override
    public RowType rowType()
    {
        return indexRowType;
    }

    @Override
    public ValueSource eval(int i) 
    {
        IndexColumn column = 
            i < keyColumns.length
            ? keyColumns[i]
            : valueColumns[i - keyColumns.length];
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
        this.keyColumns = new IndexColumn[index().getKeyColumns().size()];
        this.valueColumns = new IndexColumn[index().getValueColumns().size()];
        index().getKeyColumns().toArray(this.keyColumns);
        index().getValueColumns().toArray(this.valueColumns);
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
        return keySources.get(i);
    }
    
    // Object state

    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private IndexColumn[] keyColumns;
    private IndexColumn[] valueColumns;
    private final SparseArray<PersistitKeyValueSource> keySources = new SparseArray<PersistitKeyValueSource>() {
        @Override
        protected PersistitKeyValueSource initialValue() {
            return new PersistitKeyValueSource();
        }
    };
    private final Key indexRow;
    private PersistitHKey hKey;
}
