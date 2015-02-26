/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.store.MemoryStoreData;

public class MemoryGroupCursor extends RowCursorImpl implements GroupCursor
{
    private final MemoryAdapter adapter;
    private final MemoryStoreData storeData;
    private final Schema schema;
    private HKey hKey;
    private boolean hKeyDeep;
    private GroupScan groupScan;

    public MemoryGroupCursor(MemoryAdapter adapter, Group group) {
        this.adapter = adapter;
        this.storeData = adapter.getUnderlyingStore()
                                .createStoreData(adapter.getSession(), group);
        this.schema = SchemaCache.globalSchema(group.getAIS());
    }

    @Override
    public void open() {
        super.open();
        if(hKey == null) {
            groupScan = new FullScan();
        } else if(hKeyDeep) {
            groupScan = new HKeyAndDescendantScan(hKey);
        } else {
            groupScan = new HKeyWithoutDescendantScan(hKey);
        }
    }

    @Override
    public MemoryGroupRow next() {
        CursorLifecycle.checkIdleOrActive(this);
        boolean next = isActive();
        MemoryGroupRow row = null;
        if(next) {
            groupScan.advance();
            next = isActive();
            if (next) {
                Row tempRow = adapter.getUnderlyingStore().expandGroupData(adapter.getSession(), storeData, schema);
                row = new MemoryGroupRow(adapter.getKeyCreator(), tempRow, storeData.persistitKey);
            }
        }
        return row;
    }

    @Override
    public void close() {
        groupScan = null;
        super.close();
    }

    @Override
    public void rebind(HKey hKey, boolean deep) {
        CursorLifecycle.checkClosed(this);
        this.hKey = hKey;
        this.hKeyDeep = deep;
    }

    //
    // Internal
    //

    private abstract class GroupScan {
        public void advance() {
            if(!storeData.next()) {
                setIdle();
            }
        }
    }

    private class FullScan extends GroupScan
    {
        public FullScan() {
            adapter.getUnderlyingStore().groupIterator(adapter.getSession(), storeData);
        }
    }

    private class HKeyAndDescendantScan extends GroupScan
    {
        public HKeyAndDescendantScan(HKey hKey) {
            hKey.copyTo(storeData.persistitKey.clear());
            adapter.getUnderlyingStore().groupKeyAndDescendantsIterator(adapter.getSession(), storeData);
        }
    }

    private class HKeyWithoutDescendantScan extends GroupScan
    {
        boolean first = true;

        @Override
        public void advance()
        {
            if(first) {
                super.advance();
                first = false;
            } else {
                setIdle();
            }
        }

        public HKeyWithoutDescendantScan(HKey hKey)
        {
            storeData.persistitKey.clear();
            hKey.copyTo(storeData.persistitKey);
            adapter.getUnderlyingStore().groupKeyIterator(adapter.getSession(), storeData);
        }
    }
}
