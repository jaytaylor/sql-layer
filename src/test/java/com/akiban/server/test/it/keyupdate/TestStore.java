/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.Group;
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
                           columnSelector,
                           null);
        TestRow currentRow = map.remove(oldRow.hKey());
        TestRow mergedRow = mergeRows(currentRow, newRow, columnSelector);
        map.put(mergedRow.hKey(), mergedRow);
    }

    public void traverse(Session session, Group group, TreeRecordVisitor testVisitor, TreeRecordVisitor realVisitor)
        throws Exception
    {
        persistitStore.traverse(session, group, realVisitor);
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
            mergedRow = new TestRow(newRow.getRowDef().getRowDefId(), newRow.getRowDef(), currentRow.getStore());
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
