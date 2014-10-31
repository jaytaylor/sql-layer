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

import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.store.Store;

public class KeyUpdateRow extends NiceRow
{
    public KeyUpdateRow(int tableId, RowDef rowDef, Store store)
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

    public KeyUpdateRow parent()
    {
        return parent;
    }

    public void parent(KeyUpdateRow parent)
    {
        this.parent = parent;
    }

    public Store getStore() {
        return store;
    }

    private HKey hKey;
    private KeyUpdateRow parent;
    private final Store store;
}
