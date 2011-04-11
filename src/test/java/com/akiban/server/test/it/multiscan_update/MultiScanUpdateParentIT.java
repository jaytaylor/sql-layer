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

package com.akiban.server.test.it.multiscan_update;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.scan.ConcurrentScanAndUpdateException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.test.ApiTestBase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * <p>A special and more subtle case of the standard "scans and updates interleaved" problem.</p>
 *
 * <p>Imagine a COI where the items table has a field "quantity int" and a non-unique index on it. An update comes
 * in as:
 * <pre>UPDATE orders SET cid=? WHERE oid IN (SELECT oid FROM items WHERE quantity > ?)</pre></p>
 *
 * <p>One way this could be realized is by doing an index scan on {@code items.quantity}, then for each row that
 * comes back, doing a scan on {@code orders WHERE oid=<oid from the items row>}, and for each of those, doing the
 * update. In this case, the update to {@code orders} affects (and thus invalidates) the HKey on {@code items},
 * which invalidates the scan on {@code items.quantity}.</p>
 *
 * <p>We'll test this with an even simpler situation, one which can't come from the AAM but which covers the above.
 * We'll open a scan against {@code items.quantity}, then update an order (which happens to not be unrelated to
 * any item), then get rows from the scan and confirm the exception.</p>
 */
public final class MultiScanUpdateParentIT extends ApiTestBase {
    private static final String SCHEMA = "sc";
    private int cId;
    private int oId;
    private int iId;

    @Before
    public void setUp() throws InvalidOperationException {
        cId = createTable(SCHEMA, "c", "cid int key");
        oId = createTable(SCHEMA, "o",
                "oid int key", "cid int",
                "CONSTRAINT __akiban_o FOREIGN KEY __akiban_o(cid) REFERENCES c(cid)");
        iId = createTable(SCHEMA, "i",
                "iid int key", "oid int", "quantity int",
                "KEY (quantity)",
                "CONSTRAINT __akiban_i FOREIGN KEY __akiban_i(oid) REFERENCES o(oid)");
        writeRows(
                createNewRow(cId, 1),
                createNewRow(cId, 2),
                createNewRow(cId, 3),
                createNewRow(oId, 1, 1),
                createNewRow(oId, 2, 2),
                createNewRow(iId, 1, 1, 5)
        );
    }

    @Test(expected=ConcurrentScanAndUpdateException.class)
    public void updatedOrderInvalidatesItemScan() throws InvalidOperationException {
        ListRowOutput output = new ListRowOutput();
        CursorId cursorId;
        try {
            cursorId = dml().openCursor(session(), scanAllRequest(iId));
            dml().updateRow(
                    session(),
                    createNewRow(oId, 2, 2),
                    createNewRow(oId, 2, 3),
                    ALL_COLUMNS
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        try {
            dml().scanSome(session(), cursorId, output);
        } catch (ConcurrentScanAndUpdateException e) {
            assertEquals("rows scanned", 0, output.getRows().size());
            throw e;
        }
    }

    @Test(expected=ConcurrentScanAndUpdateException.class)
    public void updatedCustomerInvalidatesItemScan() throws InvalidOperationException {
        ListRowOutput output = new ListRowOutput();
        CursorId cursorId;
        try {
            cursorId = dml().openCursor(session(), scanAllRequest(iId));
            dml().updateRow(
                    session(),
                    createNewRow(cId, 3),
                    createNewRow(cId, 4),
                    ALL_COLUMNS
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        try {
            dml().scanSome(session(), cursorId, output);
        } catch (ConcurrentScanAndUpdateException e) {
            assertEquals("rows scanned", 0, output.getRows().size());
            throw e;
        }
    }
}
