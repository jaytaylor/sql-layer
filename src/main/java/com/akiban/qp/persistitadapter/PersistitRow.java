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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.TableRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.encoding.EncodingException;
import com.persistit.Exchange;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

class PersistitRow extends TableRow
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

    // TableRow interface

    @Override
    public UserTable table()
    {
        return (UserTable) row.getRowDef().table();
    }

    // PersistitRow interface

    public PersistitHKey hKey()
    {
        return hKey;
    }

    public void copyFromExchange(Exchange exchange) throws InvalidOperationException, PersistitException
    {
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
                byte[] buffer = rowData.getBytes();
                int newSize = buffer.length == 0 ? INITIAL_ROW_SIZE : buffer.length * 2;
                rowData = new RowData(new byte[newSize]);
                row.setRowData(rowData);
            }
        } while (exception != null);
    }

    public PersistitRow(PersistitAdapter adapter)
    {
        this.adapter = adapter;
        this.rowData = new RowData(EMPTY_BYTE_ARRAY);
        this.row = new LegacyRowWrapper((RowDef) null);
        this.hKey = new PersistitHKey();
    }

    // For use by this package

    PersistitRow(PersistitRow row)
    {
        this.adapter = row.adapter;
        this.rowData = row.rowData.copy();
        this.row = new LegacyRowWrapper(this.rowData);
        this.row.setRowDef(this.rowData.getRowDefId());
        this.hKey = row.hKey.copy();
    }

    // Class state

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final int INITIAL_ROW_SIZE = 500;

    // Object state

    private final PersistitAdapter adapter;
    private RowData rowData;
    private LegacyRowWrapper row;
    private PersistitHKey hKey;
}
