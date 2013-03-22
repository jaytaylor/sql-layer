
package com.akiban.server.test.it.keyupdate;

import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;

public class TestRow extends NiceRow
{
    public TestRow(int tableId, RowDef rowDef, Store store)
    {
        super(tableId, rowDef);
        this.store = store;
    }

    public HKey hKey()
    {
        return hKey;
    }

    public void hKey(HKey hKey)
    {
        this.hKey = hKey;
    }

    public TestRow parent()
    {
        return parent;
    }

    public void parent(TestRow parent)
    {
        this.parent = parent;
    }

    public Store getStore() {
        return store;
    }

    private HKey hKey;
    private TestRow parent;
    private final Store store;
}
