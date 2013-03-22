
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
