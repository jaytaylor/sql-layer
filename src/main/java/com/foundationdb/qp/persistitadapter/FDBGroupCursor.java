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
package com.foundationdb.qp.persistitadapter;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.KeyValue;
import com.foundationdb.async.AsyncIterator;
import com.persistit.Key;

import java.util.Iterator;

public class FDBGroupCursor implements GroupCursor {
    private final FDBAdapter adapter;
    private final Group group;
    private final FDBStoreData storeData;
    private PersistitHKey hKey;
    private boolean hKeyDeep;
    private GroupScan groupScan;
    private boolean idle;
    private boolean destroyed;
    private int commitFrequency;

    public FDBGroupCursor(FDBAdapter adapter, Group group) {
        this.adapter = adapter;
        this.group = group;
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
                KeyValue kv = groupScan.getCurrent();
                RowData rowData = new RowData();
                adapter.getUnderlyingStore().expandGroupData(storeData, rowData, kv);
                row = new FDBGroupRow(adapter);
                row.set(storeData.key, rowData);
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


    private interface GroupScan {
        void advance();
        KeyValue getCurrent();
    }

    private class FullScan implements GroupScan {
        private final Iterator<KeyValue> it;
        private KeyValue current;

        public FullScan() {
            it = adapter.getUnderlyingStore().groupIterator(adapter.getSession(), group);
        }

        @Override
        public void advance() {
            if(it.hasNext()) {
                current = it.next();
            } else {
                close();
                current = null;
            }
        }

        @Override
        public KeyValue getCurrent() {
            return current;
        }
    }

    private class HKeyAndDescendantScan implements GroupScan {
        private final Iterator<KeyValue> it;
        private KeyValue current;

        public HKeyAndDescendantScan(PersistitHKey hKey) {
            it = adapter.getUnderlyingStore().groupIterator(adapter.getSession(), group, hKey.key());
        }

        @Override
        public void advance() {
            if(it.hasNext()) {
                current = it.next();
            } else {
                close();
                current = null;
            }
        }

        @Override
        public KeyValue getCurrent() {
            return current;
        }
    }

    private class HKeyWithoutDescendantScan implements GroupScan
    {
        private final Iterator<KeyValue> it;
        boolean first = true;
        private KeyValue current;

        @Override
        public void advance()
        {
            current = null;
            if(first) {
                if(it.hasNext()) {
                    current = it.next();
                } else {
                    FDBGroupCursor.this.close();
                }
                first = false;
            } else {
                close();
            }
        }

        @Override
        public KeyValue getCurrent() {
            return current;
        }

        public HKeyWithoutDescendantScan(PersistitHKey hKey)
        {
            it = adapter.getUnderlyingStore().groupIterator(adapter.getSession(), group, hKey.key());
        }
    }

    private class CommittingFullScan implements GroupScan {
        private Iterator<KeyValue> it;
        private KeyValue current;
        private boolean auto;
        private int limit, remaining;

        public CommittingFullScan() {
            auto = (commitFrequency == StoreAdapter.COMMIT_FREQUENCY_PERIODICALLY);
            if (auto)
                limit = adapter.getTransaction().periodicallyCommitScanLimit();
            else
                limit = commitFrequency;
        }

        @Override
        public void advance() {
            if (it == null) {
                it = adapter.getUnderlyingStore().groupIterator(adapter.getSession(), group,
                                                                limit, current);
                remaining = limit;
            }
            if (it.hasNext()) {
                current = it.next();
                remaining--;
                if ((remaining <= 0) || 
                    (auto && adapter.getTransaction().timeToCommit())) {
                    ((AsyncIterator)it).dispose();
                    it = null;
                    adapter.getTransaction().commitAndReset(adapter.getSession());
                }
            }
            else {
                close();
                current = null;
            }
        }

        @Override
        public KeyValue getCurrent() {
            return current;
        }
    }

}
