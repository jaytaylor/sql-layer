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

import java.util.List;

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.value.Value;

public class KeyUpdateRow extends ValuesHolderRow
{
    public KeyUpdateRow(RowType type, Store store, Object...objects) 
    {
        super (type, objects);
        this.store = store;
    }
    
    public KeyUpdateRow(RowType type, Store store, List<Value> values) {
        super (type, values);
        this.store = store;
    }

    public com.foundationdb.server.test.it.keyupdate.HKey keyUpdateHKey()
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
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        
        if (obj instanceof Row) {
            return this.compareTo((Row)obj, 0, 0, rowType().nFields()) == 0;
        }
        return false;
    }

    private com.foundationdb.server.test.it.keyupdate.HKey hKey;
    private KeyUpdateRow parent;
    private final Store store;
}
