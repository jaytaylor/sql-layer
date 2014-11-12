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
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;

public class FDBGroupCursor extends RowCursorImpl implements GroupCursor {
    private final FDBAdapter adapter;
    private final FDBStoreData storeData;
    private HKey hKey;
    private boolean hKeyDeep;
    private GroupScan groupScan;
    private int commitFrequency;
    // static state
    private static final PointTap TRAVERSE_COUNT = Tap.createCount("traverse: fdb group cursor");


    public FDBGroupCursor(FDBAdapter adapter, Group group) {
        this.adapter = adapter;
        this.storeData = adapter.getUnderlyingStore()
            .createStoreData(adapter.getSession(), group);
    }

    public FDBGroupCursor(FDBAdapter adapter, Group group, int commitFrequency) {
        this(adapter, group);
        this.commitFrequency = commitFrequency;
    }

    @Override
    public void rebind(HKey hKey, boolean deep) {
        CursorLifecycle.checkClosed(this);
        this.hKey = hKey;
        this.hKeyDeep = deep;
    }

    @Override
    public void open() {
        super.open();
        if(commitFrequency != 0) {
            groupScan = new CommittingFullScan();
        } else if(hKey == null) {
            groupScan = new FullScan();
        } else if(hKeyDeep) {
            groupScan = new HKeyAndDescendantScan(hKey);
        } else {
            groupScan = new HKeyWithoutDescendantScan(hKey);
        }
    }

    @Override
    public FDBGroupRow next() {
        CursorLifecycle.checkIdleOrActive(this);
        boolean next = isActive();
        FDBGroupRow row = null;
        if (next) {
            groupScan.advance();
            next = isActive();
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
    public void close() {
        groupScan = null;
        super.close();
    }

    private abstract class GroupScan {
        public void advance() {
            TRAVERSE_COUNT.hit();
            if (!storeData.next()) {
                setIdle();
            }
        }
    }

    private class FullScan extends GroupScan {
        public FullScan() {
            adapter.getUnderlyingStore().groupIterator(adapter.getSession(), storeData);
        }
    }

    private class HKeyAndDescendantScan extends GroupScan {
        public HKeyAndDescendantScan(HKey hKey) {
            hKey.copyTo(storeData.persistitKey.clear());
            adapter.getUnderlyingStore().groupKeyAndDescendantsIterator(adapter.getSession(), storeData, false);
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
            hKey.copyTo(storeData.persistitKey.clear());
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
                setIdle();
            }
        }
    }

}
