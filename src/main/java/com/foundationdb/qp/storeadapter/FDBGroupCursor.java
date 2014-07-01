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
package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;

public class FDBGroupCursor implements GroupCursor {
    private final FDBAdapter adapter;
    private final FDBStoreData storeData;
    private PersistitHKey hKey;
    private boolean hKeyDeep;
    private GroupScan groupScan;
    private boolean idle;
    private boolean destroyed;
    private int commitFrequency;
    // static state
    private static final PointTap TRAVERSE_COUNT = Tap.createCount("traverse: fdb group cursor");


    public FDBGroupCursor(FDBAdapter adapter, Group group) {
        this.adapter = adapter;
        this.storeData = adapter.getUnderlyingStore()
            .createStoreData(adapter.getSession(), group);
        this.idle = true;
        this.destroyed = false;
    }

    public FDBGroupCursor(FDBAdapter adapter, Group group, int commitFrequency) {
        this(adapter, group);
        this.commitFrequency = commitFrequency;
    }

    @Override
    public void rebind(HKey hKey, boolean deep) {
        CursorLifecycle.checkIdle(this);
        this.hKey = (PersistitHKey)hKey;
        this.hKeyDeep = deep;
    }

    @Override
    public void open() {
        CursorLifecycle.checkIdle(this);
        if(commitFrequency != 0) {
            groupScan = new CommittingFullScan();
        } else if(hKey == null) {
            groupScan = new FullScan();
        } else if(hKeyDeep) {
            groupScan = new HKeyAndDescendantScan(hKey);
        } else {
            groupScan = new HKeyWithoutDescendantScan(hKey);
        }
        idle = false;
    }

    @Override
    public FDBGroupRow next() {
        CursorLifecycle.checkIdleOrActive(this);
        boolean next = !idle;
        FDBGroupRow row = null;
        if (next) {
            groupScan.advance();
            next = !idle;
            if (next) {
                RowData rowData = new RowData();
                adapter.getUnderlyingStore().expandGroupData(adapter.getSession(), storeData, rowData);
                row = new FDBGroupRow(adapter);
                row.set(storeData.persistitKey, rowData);
            }
        }
        return row;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        CursorLifecycle.checkIdleOrActive(this);
        if (!idle) {
            groupScan = null;
            idle = true;
        }
    }

    @Override
    public void destroy() {
        destroyed = true;
    }

    @Override
    public boolean isIdle() {
        return !destroyed && idle;
    }

    @Override
    public boolean isActive() {
        return !destroyed && !idle;
    }

    @Override
    public boolean isDestroyed() {
        return destroyed;
    }


    private abstract class GroupScan {
        public void advance() {
            TRAVERSE_COUNT.hit();
            if (!storeData.next()) {
                close();
            }
        }
    }

    private class FullScan extends GroupScan {
        public FullScan() {
            adapter.getUnderlyingStore().groupIterator(adapter.getSession(), storeData);
        }
    }

    private class HKeyAndDescendantScan extends GroupScan {
        public HKeyAndDescendantScan(PersistitHKey hKey) {
            hKey.key().copyTo(storeData.persistitKey);
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
                close();
            }
        }

        public HKeyWithoutDescendantScan(PersistitHKey hKey)
        {
            hKey.key().copyTo(storeData.persistitKey);
            adapter.getUnderlyingStore().groupKeyIterator(adapter.getSession(), storeData);
        }
    }

    private class CommittingFullScan extends GroupScan {
        boolean first = true;
        boolean auto;
        int limit, remaining;

        public CommittingFullScan() {
            auto = (commitFrequency == StoreAdapter.COMMIT_FREQUENCY_PERIODICALLY);
            if (auto)
                limit = adapter.getTransaction().periodicallyCommitScanLimit();
            else
                limit = commitFrequency;
        }

        @Override
        public void advance() {
            if (storeData.iterator == null) {
                adapter.getUnderlyingStore().groupIterator(adapter.getSession(), storeData,
                                                           !first, limit);
                first = false;
                remaining = limit;
            }
            TRAVERSE_COUNT.hit();
            if (storeData.next()) {
                remaining--;
                if ((remaining <= 0) || 
                    (auto && adapter.getTransaction().timeToCommit())) {
                    storeData.closeIterator();
                    adapter.getTransaction().commitAndReset(adapter.getSession());
                }
            }
            else {
                close();
            }
        }
    }

}
