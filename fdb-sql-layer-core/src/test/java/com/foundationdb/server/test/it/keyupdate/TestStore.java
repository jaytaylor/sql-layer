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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.IndexKeyVisitor;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.TreeRecordVisitor;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

// TestStore is Store-like. Not worth the effort to make it completely compatible.

public class TestStore
{
    // Store-ish interface. Update delegate first, so that if it throws an exception, map is left alone.

    public void writeRow(Session session, KeyUpdateRow row)
        throws Exception
    {
        realStore.writeRow(session, row.toRowData());
        map.put(row.hKey(), row);
    }

    public void deleteRow(Session session, KeyUpdateRow row)
        throws Exception
    {
        realStore.deleteRow(session, row.toRowData(), false);
        map.remove(row.hKey());
    }

    public void updateRow(Session session, KeyUpdateRow oldRow, KeyUpdateRow newRow, ColumnSelector columnSelector)
    {
        realStore.updateRow(session,
                            oldRow.toRowData(),
                            newRow.toRowData(), // Not mergedRow. Rely on delegate to merge existing and new.
                            null);
        KeyUpdateRow currentRow = map.remove(oldRow.hKey());
        KeyUpdateRow mergedRow = mergeRows(currentRow, newRow, columnSelector);
        map.put(mergedRow.hKey(), mergedRow);
    }

    public void traverse(Session session, Group group, TreeRecordVisitor testVisitor, TreeRecordVisitor realVisitor)
        throws Exception
    {
        realStore.traverse(session, group, realVisitor);
        for (Map.Entry<HKey, KeyUpdateRow> entry : map.entrySet()) {
            testVisitor.visit(entry.getKey().objectArray(), entry.getValue());
        }
    }

    public void traverse(Session session, Index index, IndexKeyVisitor visitor, long scanTimeLimit, long sleepTime)
    {
        realStore.traverse(session, index, visitor, scanTimeLimit, sleepTime);
    }

    // TestStore interface

    public KeyUpdateRow find(HKey hKey)
    {
        return map.get(hKey);
    }

    public TestStore(Store realStore)
    {
        this.realStore = realStore;
    }

    public void writeTestRow(KeyUpdateRow row)
    {
        map.put(row.hKey(), row);
    }

    public void deleteTestRow(KeyUpdateRow row)
    {
        map.remove(row.hKey());
    }

    // For use by this class

    private KeyUpdateRow mergeRows(KeyUpdateRow currentRow, KeyUpdateRow newRow, ColumnSelector columnSelector)
    {
        KeyUpdateRow mergedRow;
        if (columnSelector == null) {
            mergedRow = newRow;
        } else {
            if (currentRow.getRowDef() != newRow.getRowDef()) {
                throw new RuntimeException();
            }
            mergedRow = new KeyUpdateRow(newRow.getRowDef().getRowDefId(), newRow.getRowDef(), currentRow.getStore());
            int n = newRow.getRowDef().getFieldCount();
            for (int i = 0; i < n; i++) {
                mergedRow.put(i, columnSelector.includesColumn(i) ? newRow.get(i) : currentRow.get(i));
            }
        }
        return mergedRow;
    }

    // Object state

    private final SortedMap<HKey, KeyUpdateRow> map = new TreeMap<>();
    private final Store realStore;
}
