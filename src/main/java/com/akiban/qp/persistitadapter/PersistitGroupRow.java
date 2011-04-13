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

// public for access by PhysicalOperatorIT
public class PersistitGroupRow extends RowBase
{
    // Object interface

    @Override
    public String toString()
    {
        return rowData == null ? null : rowData.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return adapter.schema().userTableRowType(row.getRowDef().userTable());
    }

    @Override
    public Object field(int i)
    {
        return row.get(i);
    }

    public PersistitHKey hKey()
    {
        return currentHKey;
    }

    // PersistitGroupRow interface

    static PersistitGroupRow newPersistitGroupRow(PersistitAdapter adapter)
    {
        return new PersistitGroupRow(adapter);
    }

    // For use by PhysicalOperatorIT
    public static PersistitGroupRow newPersistitGroupRow(PersistitAdapter adapter, RowData rowData)
    {
        return new PersistitGroupRow(adapter, rowData);
    }

    // For use by this package

    void copyFromExchange(Exchange exchange) throws InvalidOperationException, PersistitException
    {
        this.row = new LegacyRowWrapper((RowDef) null);
        RuntimeException exception;
        do {
            try {
                exception = null;
                adapter.persistit.expandRowData(exchange, rowData);
                row.setRowDef(rowData.getRowDefId());
                row.setRowData(rowData);
                persistitHKey().copyFrom(exchange.getKey());
                rowData.hKey(persistitHKey().key());
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

    public RowData rowData()
    {
        return rowData;
    }

    // For use by this class

    private PersistitHKey persistitHKey()
    {
        RowDef rowDef = row.getRowDef();
        currentHKey = typedHKeys[rowDef.getOrdinal()];
        if (currentHKey == null) {
            currentHKey = new PersistitHKey(adapter, rowDef.userTable().hKey());
            typedHKeys[rowDef.getOrdinal()] = currentHKey;
        }
        return currentHKey;
    }

    private PersistitGroupRow(PersistitAdapter adapter)
    {
        this(adapter, new RowData(new byte[INITIAL_ROW_SIZE]));
    }

    private PersistitGroupRow(PersistitAdapter adapter, RowData rowData)
    {
        this.adapter = adapter;
        this.rowData = rowData;
        this.typedHKeys = new PersistitHKey[adapter.schema().maxTypeId() + 1];
    }

    // Class state

    private static final int INITIAL_ROW_SIZE = 500;

    // Object state

    private final PersistitAdapter adapter;
    private RowData rowData;
    private LegacyRowWrapper row;
    private PersistitHKey currentHKey;
    private final PersistitHKey[] typedHKeys;
}
