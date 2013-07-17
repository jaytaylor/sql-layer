/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Group;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.GroupCursor;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.store.FDBStore;
import com.foundationdb.AsyncIterator;
import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import com.persistit.Key;

import java.util.Iterator;

public class FDBGroupCursor implements GroupCursor {
    private final FDBAdapter adapter;
    private final Group group;
    private PersistitHKey hKey;
    private boolean hKeyDeep;
    private GroupScan groupScan;
    private boolean idle;
    private boolean destroyed;


    public FDBGroupCursor(FDBAdapter adapter, Group group) {
        this.adapter = adapter;
        this.group = group;
        this.idle = true;
        this.destroyed = false;
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
        if(hKey == null) {
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
                // Key
                byte[] keyBytes = Tuple.fromBytes(kv.getKey()).getBytes(2);
                Key key = adapter.createKey();
                System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
                key.setEncodedSize(keyBytes.length);
                // Value
                RowData rowData = new RowData();
                FDBStore.expandRowData(rowData, kv.getValue(), true);
                // Row
                row = new FDBGroupRow(adapter);
                row.set(key, rowData);
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
        private final AsyncIterator<KeyValue> it;
        private KeyValue current;

        public FullScan() {
            it = adapter.getUnderlyingStore().groupIterator(adapter.getSession(), group);
        }

        @Override
        public void advance() {
            if(it.hasNext().get()) {
                current = it.next().get();
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
        private final AsyncIterator<KeyValue> it;
        private KeyValue current;

        public HKeyAndDescendantScan(PersistitHKey hKey) {
            it = adapter.getUnderlyingStore().groupIterator(adapter.getSession(), group, hKey.key());
        }

        @Override
        public void advance() {
            if(it.hasNext().get()) {
                current = it.next().get();
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
        private final AsyncIterator<KeyValue> it;
        boolean first = true;
        private KeyValue current;

        @Override
        public void advance()
        {
            current = null;
            if(first) {
                if(it.hasNext().get()) {
                    current = it.next().get();
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
}
