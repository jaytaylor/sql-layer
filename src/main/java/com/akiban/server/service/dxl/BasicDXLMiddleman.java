/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.dxl;

import com.akiban.server.api.dml.scan.ColumnSet;
import com.akiban.server.api.dml.scan.Cursor;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.service.session.Session;

import java.util.Map;
import java.util.Set;

public final class BasicDXLMiddleman {
    private static final Session.MapKey<CursorId,ScanData> CURSORS_TO_SCANDATA = Session.MapKey.mapNamed("CURSORS_TO_SCANDATA");

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

    static ScanData putScanData(Session session, CursorId cursorId, ScanData scanData) {
        return session.put(CURSORS_TO_SCANDATA, cursorId, scanData);
    }

    static ScanData getScanData(Session session, CursorId cursorId) {
        return session.get(CURSORS_TO_SCANDATA, cursorId);
    }

    static ScanData removeScanData(Session session, CursorId cursorId) {
        return session.remove(CURSORS_TO_SCANDATA, cursorId);
    }

    static Map<CursorId,ScanData> getScanDataMap(Session session) {
        return session.get(CURSORS_TO_SCANDATA);
    }
}
