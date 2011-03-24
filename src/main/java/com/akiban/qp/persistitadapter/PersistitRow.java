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

import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.encoding.EncodingException;
import com.persistit.Exchange;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

// Gets row state from PersistitCursor if possible. Before the cursor needs to move on, state is copied.

class PersistitRow extends RowBase
{
    // Object interface

    @Override
    public String toString()
    {
        return row.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return adapter.schema.userTableRowType(row.getRowDef().userTable());
    }

    @Override
    public <T> T field(int i)
    {
        return (T) row.get(i);
    }

    // For use by this package

    public void copyFromExchange(Exchange exchange) throws InvalidOperationException, PersistitException
    {
        this.row = new LegacyRowWrapper((RowDef) null);
        this.hKey = new PersistitHKey();
        RuntimeException exception;
        Value value = exchange.getValue();
        assert value.isDefined();
        do {
            try {
                exception = null;
                adapter.persistit.expandRowData(exchange, rowData);
                row.setRowDef(rowData.getRowDefId());
                row.setRowData(rowData);
                hKey.readKey(exchange);
            } catch (ArrayIndexOutOfBoundsException e) {
                exception = e;
            } catch (EncodingException e) {
                if (e.getCause() instanceof ArrayIndexOutOfBoundsException) {
                    exception = e;
                } else {
                    throw e;
                }
            }
            if (exception != null) {
                rowData.reset(new byte[rowData.getBytes().length * 2]);
            }
        } while (exception != null);
    }

    public PersistitHKey hKey()
    {
        return hKey;
    }


    PersistitRow(PersistitAdapter adapter)
    {
        this.adapter = adapter;
        this.rowData = new RowData(new byte[INITIAL_ROW_SIZE]);
    }

    // Class state

    private static final int INITIAL_ROW_SIZE = 500;

    // Object state

    private final PersistitAdapter adapter;
    private RowData rowData;
    private LegacyRowWrapper row;
    private PersistitHKey hKey;
}
