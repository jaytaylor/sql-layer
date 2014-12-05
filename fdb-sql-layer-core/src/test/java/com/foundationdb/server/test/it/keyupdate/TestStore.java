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
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.IndexKeyVisitor;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.TreeRecordVisitor;
import com.foundationdb.server.types.value.Value;

import java.util.ArrayList;
import java.util.List;
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
        realStore.writeRow(session, row, null, null);
        map.put(row.keyUpdateHKey(), row);
    }

    public void deleteRow(Session session, KeyUpdateRow row)
        throws Exception
    {
        realStore.deleteRow(session, row, false);
        map.remove(row.keyUpdateHKey());
    }

    public void updateRow(Session session, KeyUpdateRow oldRow, KeyUpdateRow newRow, ColumnSelector columnSelector)
    {
        realStore.updateRow(session,
                            oldRow,
                            newRow);
        KeyUpdateRow currentRow = map.remove(oldRow.keyUpdateHKey());
        assert currentRow != null;
        //KeyUpdateRow mergedRow = mergeRows(currentRow, newRow, columnSelector);
        map.put(newRow.keyUpdateHKey(), newRow);
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
        map.put(row.keyUpdateHKey(), row);
    }

    public void deleteTestRow(KeyUpdateRow row)
    {
        map.remove(row.keyUpdateHKey());
    }

    // For use by this class

    // Object state

    private final SortedMap<HKey, KeyUpdateRow> map = new TreeMap<>();
    private final Store realStore;
}
