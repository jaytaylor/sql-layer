/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.dxl;

import com.akiban.server.api.dml.scan.ColumnSet;
import com.akiban.server.api.dml.scan.Cursor;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.service.session.Session;

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

    private final ConcurrentMap<CursorId,ScanData> openScansMap = new ConcurrentHashMap<CursorId, ScanData>();

    private static final AtomicReference<BasicDXLMiddleman> lastInstance = new AtomicReference<BasicDXLMiddleman>();
}
