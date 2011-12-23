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

import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.encoding.EncodingException;
import com.akiban.server.types.ValueSource;
import com.akiban.util.SparseArray;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// public for access by OperatorIT
public class PersistitGroupRow extends AbstractRow
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
        return adapter.schema().userTableRowType(rowDef().userTable());
    }

    @Override
    public ValueSource eval(int i) {
        FieldDef fieldDef = rowDef().getFieldDef(i);
        RowData rowData = rowData();
        RowDataValueSource valueSource = valueSource(i);
        valueSource.bind(fieldDef, rowData);
        return valueSource;
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

    // For use by OperatorIT
    public static PersistitGroupRow newPersistitGroupRow(PersistitAdapter adapter, RowData rowData)
    {
        return new PersistitGroupRow(adapter, rowData);
    }

    // For use by this package

    RowDef rowDef() {
        if (row != null)
            return row.getRowDef();
        if (rowData != null) {
            return adapter.rowDef(rowData.getRowDefId());
        }
        throw new IllegalStateException("no active row");
    }

    void copyFromExchange(Exchange exchange) throws PersistitException
    {
        this.row = new LegacyRowWrapper((RowDef) null);
        RuntimeException exception;
        do {
            try {
                exception = null;
                adapter.persistit().expandRowData(exchange, rowData);
                row.setRowDef(rowData.getRowDefId(), adapter.persistit());
                row.setRowData(rowData);
                PersistitHKey persistitHKey = persistitHKey();
                persistitHKey.copyFrom(exchange.getKey());
                rowData.hKey(persistitHKey.key());
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
                int newSize = rowData.getBytes().length * 2;
                if (newSize >= MAX_ROWDATA_SIZE_BYTES) {
                    LOG.error("{}: Unable to copy from exchange for key {}: {}",
                              new Object[]{this, exchange.getKey(), exception.getMessage()});
                    throw exception;
                }
                rowData.reset(new byte[newSize]);
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
        int ordinal = rowDef.getOrdinal();
        if (ordinal >= typedHKeys.length) {
            PersistitHKey[] newTypedHKeys = new PersistitHKey[ordinal * 2];
            System.arraycopy(typedHKeys, 0, newTypedHKeys, 0, typedHKeys.length);
            typedHKeys = newTypedHKeys;
        }
        currentHKey = typedHKeys[ordinal];
        if (currentHKey == null) {
            currentHKey = new PersistitHKey(adapter, rowDef.userTable().hKey());
            typedHKeys[ordinal] = currentHKey;
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
        this.typedHKeys = new PersistitHKey[INITIAL_ARRAY_SIZE];
    }
    
    private RowDataValueSource valueSource(int i)
    {
        return valueSources.get(i);
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(PersistitGroupRow.class);
    private static final int INITIAL_ROW_SIZE = 500;
    private static final int INITIAL_ARRAY_SIZE = 10;
    private static final int MAX_ROWDATA_SIZE_BYTES = 5000000;

    // Object state

    private final SparseArray<RowDataValueSource> valueSources = new SparseArray<RowDataValueSource>() {
        @Override
        protected RowDataValueSource createNew() {
            return new RowDataValueSource();
        }
    };
    private final PersistitAdapter adapter;
    private RowData rowData;
    private LegacyRowWrapper row;
    private PersistitHKey currentHKey;
    private PersistitHKey[] typedHKeys;
}
