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
import com.akiban.ais.model.UserTable;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.IndexDef;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

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
/*
        IndexDef indexDef = indexDef();
        indexRow.indexTo(i);
        int from = indexRow.getIndex();
        indexRow.indexTo(i + 1);
        int to = indexRow.getIndex();
        indexRow.getEncodedBytes();
        FieldDef fieldDef = indexDef.getRowDef().getFieldDef(indexDef.getFields()[i]);
        Encoding encoding = fieldDef.getEncoding();
*/
        assert false : "Not implemented yet";
        return null;
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
        this.indexRow = adapter.persistit.getKey(adapter.session);
        UserTable userTable = (UserTable) indexRowType.index().getTable();
        this.hKey = new PersistitHKey(adapter, userTable.hKey());
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
    private final Key indexRow;
    private PersistitHKey hKey;
}
