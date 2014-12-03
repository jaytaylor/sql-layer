/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store;

import com.foundationdb.ais.model.HKey;
import com.foundationdb.ais.model.HKeySegment;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.session.Session;
import com.persistit.Key;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TreeRecordVisitor
{
    public final void initialize(Session session, Store store)
    {
        this.session = session;
        this.store = store;
        for (Table table : store.getAIS(session).getTables().values()) {
            if (!table.getName().getSchemaName().equals(TableName.INFORMATION_SCHEMA) &&
                !table.getName().getSchemaName().equals(TableName.SECURITY_SCHEMA)) {
                ordinalToTable.put(table.getOrdinal(), table);
            }
        }
    }


    public void visit(Key key, Row row)  {
        Object[] keyObjs = key(key, row);
        visit(keyObjs, row);
    }
    
    public abstract void visit(Object[] key, Row row);
    
    
    private Object[] key(Key key, Row row) {
        // Key traversal
        int keySize = key.getDepth();
        // HKey traversal
        HKey hKey = row.rowType().table().hKey();
        List<HKeySegment> hKeySegments = hKey.segments();
        int k = 0;
        // Traverse key, guided by hKey, populating result
        Object[] keyArray = new Object[keySize];
        int h = 0;
        key.indexTo(0);
        while (k < hKeySegments.size()) {
            HKeySegment hKeySegment = hKeySegments.get(k++);
            Table table = hKeySegment.table();
            int ordinal = (Integer) key.decode();
            assert ordinalToTable.get(ordinal) == table : ordinalToTable.get(ordinal);
            keyArray[h++] = table;
            for (int i = 0; i < hKeySegment.columns().size(); i++) {
                keyArray[h++] = key.decode();
            }
        }
        return keyArray;
        
    }

    private Store store;
    private Session session;
    private final Map<Integer, Table> ordinalToTable = new HashMap<>();
}
