package com.akiban.qp.persistitadapter.indexrow;

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.IndexRowType;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public class PersistitGroupIndexRow extends PersistitIndexRow
{
    // RowBase interface

    @Override
    public HKey ancestorHKey(UserTable table)
    {
        PersistitHKey ancestorHKey = hKeyCache.hKey(table);
        constructHKeyFromIndexKey(ancestorHKey.key(), index.indexToHKey(table.getDepth()));
        return ancestorHKey;
    }

    // PersistitIndexRow interface

    public IndexToHKey indexToHKey()
    {
        return index.indexToHKey(index.leafMostTable().getDepth());
    }

    public long tableBitmap()
    {
        return tableBitmap;
    }

    public void copyFromExchange(Exchange exchange) throws PersistitException
    {
        super.copyFromExchange(exchange);
        tableBitmap = exchange.getValue().getLong();
    }

    // PersistitGroupIndexRow interface

    public PersistitGroupIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
    {
        super(adapter, indexRowType);
        this.index = (GroupIndex) indexRowType.index();
    }

    // Object state

    private final GroupIndex index;
    private long tableBitmap;
}
