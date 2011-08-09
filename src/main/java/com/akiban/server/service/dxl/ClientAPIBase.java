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

import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;

import java.util.Map;

abstract class ClientAPIBase {

    private final Store store;
    private final SchemaManager schemaManager;
    private final TreeService treeService;
    private final BasicDXLMiddleman middleman;

    ClientAPIBase(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store, TreeService treeService) {
        this.middleman = middleman;
        this.schemaManager = schemaManager;
        this.store = store;
        this.treeService = treeService;
    }

    final public Store store() {
        return store;
    }

    final public SchemaManager schemaManager() {
        return schemaManager;
    }

    final public TreeService treeService() {
        return treeService;
    }

    /**
     * Returns an exception as an InvalidOperationException. If the given
     * exception is one that we know how to turn into a specific
     * InvalidOperationException (e.g., NoSuchRowException), the returned
     * exception will be of that type. Otherwise, if the given exception is an
     * InvalidOperationException, we'll just return it, and if not, we'll wrap
     * it in a GenericInvalidOperationException.
     * 
     * @param e
     *            the exception to wrap
     * @return as specific an InvalidOperationException as we know how to make
     */
    //protected static InvalidOperationException launder(Exception e) {
    //}

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
