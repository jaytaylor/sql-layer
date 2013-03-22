
package com.akiban.qp.row;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;

public class HKeyRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return hKey.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource eval(int i)
    {
        return hKey.eval(i);
    }

    @Override
    public PValueSource pvalue(int i) {
        return hKey.pEval(i);
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    @Override
    public HKey ancestorHKey(UserTable table)
    {
        // TODO: This does the wrong thing for hkeys derived from group index rows!
        // TODO: See bug 997746.
        HKey ancestorHKey = hKeyCache.hKey(table);
        hKey.copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        throw new UnsupportedOperationException();
    }

    // HKeyRow interface

    public HKeyRow(HKeyRowType rowType, HKey hKey, HKeyCache<HKey> hKeyCache)
    {
        this.hKeyCache = hKeyCache;
        this.rowType = rowType;
        this.hKey = hKey;
    }
    
    // Object state

    private final HKeyCache<HKey> hKeyCache;
    private final HKeyRowType rowType;
    private HKey hKey;
}
