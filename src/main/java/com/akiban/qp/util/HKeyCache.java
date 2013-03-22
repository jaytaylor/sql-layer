
package com.akiban.qp.util;

// Caches HKeys. The caching isn't to cache the values -- operators take care of that. The purpose of this
// class is to maximize reuse of HKey objects.

import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.HKey;
import com.akiban.util.SparseArray;

public class HKeyCache<HKEY extends HKey>
{
    public HKEY hKey(UserTable table)
    {
        HKEY hKey;
        int ordinal = table.rowDef().getOrdinal();
        if (ordinalToHKey.isDefined(ordinal)) {
            hKey = ordinalToHKey.get(ordinal);
        } else {
            hKey = (HKEY) adapter.newHKey(table.hKey());
            ordinalToHKey.set(ordinal, hKey);
        }
        return hKey;
    }

    public HKeyCache(StoreAdapter adapter)
    {
        this.adapter = adapter;
    }

    private final StoreAdapter adapter;
    private final SparseArray<HKEY> ordinalToHKey = new SparseArray<>();
}
