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
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitKeyConversionSource;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.types.ConversionSource;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import java.util.Iterator;

public class PersistitIndexRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return hKey == null ? null : hKey.toString();
    }

    // RowBase interface

    @Override
    public RowType rowType()
    {
        return indexRowType;
    }

    @Override
    public Object field(int i, Bindings bindings)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConversionSource conversionSource(int i, Bindings bindings) {
        IndexColumn column = index().getColumns().get(i);
        conversionSource.attach(indexRow, column);
        return conversionSource;
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    // For use by PhysicalOperatorIT
    public PersistitIndexRow(PersistitAdapter adapter, IndexRowType indexRowType, Object... values) throws PersistitException
    {
        this(adapter, indexRowType);
        Iterator<IndexColumn> columnIt = index().getColumns().iterator();
        for(Object o : values) {
            FieldDef def = (FieldDef) columnIt.next().getColumn().getFieldDef();
            def.getEncoding().toKey(def, o, indexRow);
        }
    }

    // PersistitIndexRow interface

    public PersistitIndexRow(PersistitAdapter adapter, IndexRowType indexRowType) throws PersistitException
    {
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        this.indexRow = adapter.persistit.getKey(adapter.session);
        this.hKey = new PersistitHKey(adapter, index().hKey());
        this.conversionSource = new PersistitKeyConversionSource();
    }

    // For use by this package

    void copyFromExchange(Exchange exchange)
    {
        // Extract the hKey from the exchange, using indexRow as a convenient Key to bridge Exchange
        // and PersistitHKey.
        adapter.persistit.constructHKeyFromIndexKey(indexRow, exchange.getKey(), index());
        hKey.copyFrom(indexRow);
        // Now copy the entire index record into indexRow.
        exchange.getKey().copyTo(indexRow);
    }

    // For use by this class

    private Index index()
    {
        return indexRowType.index();
    }

    // Object state

    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private final PersistitKeyConversionSource conversionSource;
    private final Key indexRow;
    private PersistitHKey hKey;
}
