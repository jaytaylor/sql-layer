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

package com.akiban.server.store;

import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TreeRecordVisitor
{
    public final void visit() throws PersistitException, InvalidOperationException
    {
        NewRow row = row();
        Object[] key = key(row.getRowDef());
        visit(key, row);
    }

    public final void initialize(PersistitStore store, Exchange exchange)
    {
        this.store = store;
        this.exchange = exchange;
        for (RowDef rowDef : store.rowDefCache.getRowDefs()) {
            if (rowDef.isUserTable()) {
                UserTable table = rowDef.userTable();
                if (!table.getName().getSchemaName().equals("akiban_information_schema")) {
                    ordinalToTable.put(rowDef.getOrdinal(), table);
                }
            }
        }
    }

    public abstract void visit(Object[] key, NewRow row);

    private NewRow row() throws PersistitException, InvalidOperationException
    {
        RowData rowData = new RowData(EMPTY_BYTE_ARRAY);
        store.expandRowData(exchange, rowData);
        return new LegacyRowWrapper(rowData, store);
    }

    private Object[] key(RowDef rowDef)
    {
        // Key traversal
        Key key = exchange.getKey();
        int keySize = key.getDepth();
        // HKey traversal
        HKey hKey = rowDef.userTable().hKey();
        List<HKeySegment> hKeySegments = hKey.segments();
        int k = 0;
        // Traverse key, guided by hKey, populating result
        Object[] keyArray = new Object[keySize];
        int h = 0;
        while (k < hKeySegments.size()) {
            HKeySegment hKeySegment = hKeySegments.get(k++);
            UserTable table = hKeySegment.table();
            int ordinal = (Integer) key.decode();
            assert ordinalToTable.get(ordinal) == table : ordinalToTable.get(ordinal);
            keyArray[h++] = table;
            for (int i = 0; i < hKeySegment.columns().size(); i++) {
                keyArray[h++] = key.decode();
            }
        }
        return keyArray;
    }

    private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private PersistitStore store;
    private Exchange exchange;
    private final Map<Integer, UserTable> ordinalToTable = new HashMap<Integer, UserTable>();
}
