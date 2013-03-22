package com.akiban.qp.persistitadapter.indexrow;

import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.IndexRowType;

public class PersistitTableIndexRow extends PersistitIndexRow
{
    // RowBase interface

    @Override
    public HKey ancestorHKey(UserTable table)
    {
        PersistitHKey ancestorHKey;
        PersistitHKey leafmostHKey = hKeyCache.hKey(leafmostTable);
        if (table == leafmostTable) {
            ancestorHKey = leafmostHKey;
        } else {
            ancestorHKey = hKeyCache.hKey(table);
            leafmostHKey.copyTo(ancestorHKey);
            ancestorHKey.useSegments(table.getDepth() + 1);
        }
        return ancestorHKey;
    }

    // PersistitIndexRow interface

    @Override
    public IndexToHKey indexToHKey()
    {
        return index.indexToHKey();
    }

    // PersistitTableIndexRow interface

    public PersistitTableIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
    {
        super(adapter, indexRowType);
        this.index = (TableIndex) indexRowType.index();
    }

    // Object state

    private final TableIndex index;
}
