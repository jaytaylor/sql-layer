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

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.Index;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.IndexKeyVisitor;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.Store;
import com.akiban.server.store.TreeRecordVisitor;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

// TestStore is Store-like. Not worth the effort to make it completely compatible.

public class TestStore
{
    // Store-ish interface. Update delegate first, so that if it throws an exception, map is left alone.

    public void writeRow(Session session, TestRow row)
        throws Exception
    {
        mainDelegate.writeRow(session, row.toRowData());
        map.put(row.hKey(), row);
    }

    public void deleteRow(Session session, TestRow row)
        throws Exception
    {
        mainDelegate.deleteRow(session, row.toRowData());
        map.remove(row.hKey());
    }

    public void updateRow(Session session, TestRow oldRow, TestRow newRow, ColumnSelector columnSelector)
        throws Exception
    {
        mainDelegate.updateRow(session,
                           oldRow.toRowData(),
                           newRow.toRowData(), // Not mergedRow. Rely on delegate to merge existing and new.
                           columnSelector);
        TestRow currentRow = map.remove(oldRow.hKey());
        TestRow mergedRow = mergeRows(currentRow, newRow, columnSelector);
        map.put(mergedRow.hKey(), mergedRow);
    }

    public void traverse(Session session, RowDef rowDef, TreeRecordVisitor testVisitor, TreeRecordVisitor realVisitor)
        throws Exception
    {
        persistitStore.traverse(session, rowDef, realVisitor);
        for (Map.Entry<HKey, TestRow> entry : map.entrySet()) {
            testVisitor.visit(entry.getKey().objectArray(), entry.getValue());
        }
    }

    public void traverse(Session session, Index index, IndexKeyVisitor visitor)
        throws Exception
    {
        persistitStore.traverse(session, index, visitor);
    }

    // TestStore interface

    public TestRow find(HKey hKey)
    {
        return map.get(hKey);
    }

    public TestStore(Store mainDelegate, PersistitStore persistitStore)
    {
        this.mainDelegate = mainDelegate;
        this.persistitStore = persistitStore;
    }

    public void writeTestRow(TestRow row)
    {
        map.put(row.hKey(), row);
    }

    public void deleteTestRow(TestRow row)
    {
        map.remove(row.hKey());
    }

    // For use by this class

    private TestRow mergeRows(TestRow currentRow, TestRow newRow, ColumnSelector columnSelector)
    {
        TestRow mergedRow;
        if (columnSelector == null) {
            mergedRow = newRow;
        } else {
            if (currentRow.getRowDef() != newRow.getRowDef()) {
                throw new RuntimeException();
            }
            mergedRow = new TestRow(newRow.getRowDef().getRowDefId(), currentRow.getStore());
            int n = newRow.getRowDef().getFieldCount();
            for (int i = 0; i < n; i++) {
                mergedRow.put(i, columnSelector.includesColumn(i) ? newRow.get(i) : currentRow.get(i));
            }
        }
        return mergedRow;
    }

    // Object state

    private final SortedMap<HKey, TestRow> map = new TreeMap<HKey, TestRow>();
    private final Store mainDelegate;
    private final PersistitStore persistitStore;
}
