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

package com.foundationdb.qp.util;

// Caches HKeys. The caching isn't to cache the values -- operators take care of that. The purpose of this
// class is to maximize reuse of HKey objects.

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.util.SparseArray;

public class HKeyCache<HKEY extends HKey>
{
    @SuppressWarnings("unchecked")
    public HKEY hKey(Table table)
    {
        HKEY hKey;
        int ordinal = table.getOrdinal();
        if (ordinalToHKey.isDefined(ordinal)) {
            hKey = ordinalToHKey.get(ordinal);
        } else {
            hKey = (HKEY) adapter.newHKey(table.hKey());
            ordinalToHKey.set(ordinal, hKey);
        }
        return hKey;
    }

    public HKeyCache(KeyCreator adapter)
    {
        this.adapter = adapter;
    }

    private final KeyCreator adapter;
    private final SparseArray<HKEY> ordinalToHKey = new SparseArray<>();
}
