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

package com.foundationdb.server.service.dxl;

import com.foundationdb.server.api.dml.scan.ColumnSet;
import com.foundationdb.server.api.dml.scan.Cursor;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.ScanRequest;
import com.foundationdb.server.service.session.Session;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public final class BasicDXLMiddleman {
    static final class ScanData {
        private final Cursor cursor;
        private final byte[] scanColumns;
        private final boolean scanAll;
        private Set<Integer> scanColumnsUnpacked;

        ScanData(ScanRequest request, Cursor cursor) {
            scanColumns = request.getColumnBitMap();
            scanAll = request.scanAllColumns();
            this.cursor = cursor;
        }

        public Set<Integer> getScanColumns() {
            if (scanColumnsUnpacked == null) {
                scanColumnsUnpacked = ColumnSet.unpackFromLegacy(scanColumns);
            }
            return scanColumnsUnpacked;
        }

        public Cursor getCursor() {
            return cursor;
        }

        public boolean scanAll() {
            return scanAll;
        }

        @Override
        public String toString() {
            return String.format("ScanData[cursor=%s, columns=%s]", cursor,
                    getScanColumns());
        }
    }

    ScanData putScanData(Session session, CursorId cursorId, ScanData scanData) {
        return openScansMap.put(cursorId, scanData);
    }

    ScanData getScanData(Session session, CursorId cursorId) {
        return openScansMap.get(cursorId);
    }

    ScanData removeScanData(Session session, CursorId cursorId) {
        return openScansMap.remove(cursorId);
    }

    Map<CursorId,ScanData> getScanDataMap() {
        return openScansMap;
    }
    Map<CursorId,ScanData> getScanDataMap(Session session) {
        return openScansMap;
    }

    static BasicDXLMiddleman create() {
        BasicDXLMiddleman instance = new BasicDXLMiddleman();
        if (!lastInstance.compareAndSet(null, instance)) {
            throw new RuntimeException("there is already a BasicDXLMiddleman instance");
        }
        return instance;
    }

    static void destroy() {
        lastInstance.set(null);
    }

    static BasicDXLMiddleman last() {
        return lastInstance.get();
    }

    private BasicDXLMiddleman() {
    }

    private final ConcurrentMap<CursorId,ScanData> openScansMap = new ConcurrentHashMap<>();

    private static final AtomicReference<BasicDXLMiddleman> lastInstance = new AtomicReference<>();
}
