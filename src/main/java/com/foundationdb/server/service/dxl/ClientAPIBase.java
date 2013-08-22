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

import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;

import java.util.Map;

abstract class ClientAPIBase {

    private final Store store;
    private final SchemaManager schemaManager;
    private final BasicDXLMiddleman middleman;

    ClientAPIBase(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store) {
        this.middleman = middleman;
        this.schemaManager = schemaManager;
        this.store = store;
    }

    final public Store store() {
        return store;
    }

    final public SchemaManager schemaManager() {
        return schemaManager;
    }

    BasicDXLMiddleman.ScanData putScanData(Session session, CursorId cursorId, BasicDXLMiddleman.ScanData scanData) {
        return middleman.putScanData(session, cursorId, scanData);
    }

    BasicDXLMiddleman.ScanData getScanData(Session session, CursorId cursorId) {
        return middleman.getScanData(session, cursorId);
    }

    BasicDXLMiddleman.ScanData removeScanData(Session session, CursorId cursorId) {
        return middleman.removeScanData(session, cursorId);
    }

    Map<CursorId,BasicDXLMiddleman.ScanData> getScanDataMap() {
        return middleman.getScanDataMap();
    }
    Map<CursorId,BasicDXLMiddleman.ScanData> getScanDataMap(Session session) {
        return middleman.getScanDataMap(session);
    }

    BasicDXLMiddleman middleman() {
        return middleman;
    }
}
