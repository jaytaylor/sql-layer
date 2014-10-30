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

package com.foundationdb.server.test.it.multiscan_update;

import com.foundationdb.server.api.dml.ConstantColumnSelector;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.error.ConcurrentScanAndUpdateException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Ignore;
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
@Ignore("Testing DMLFunctions only, which is going away")
public final class MultiScanUpdateParentIT extends ITBase {
    private static final String SCHEMA = "sc";
    private int cId;
    private int oId;
    private int iId;

    @Before
    public void setUp() throws InvalidOperationException {
        cId = createTable(SCHEMA, "c", "cid int not null primary key");
        oId = createTable(SCHEMA, "o",
                "oid int not null primary key", "cid int",
                "GROUPING FOREIGN KEY (cid) REFERENCES c(cid)");
        iId = createTable(SCHEMA, "i",
                "iid int not null primary key", "oid int", "quantity int",
                "GROUPING FOREIGN KEY (oid) REFERENCES o(oid)");
        createIndex(SCHEMA, "i", "quantity", "quantity");
        writeRows(
                row(cId, 1),
                row(cId, 2),
                row(cId, 3),
                row(oId, 1, 1),
                row(oId, 2, 2),
                row(iId, 1, 1, 5)
        );
    }

    @Test(expected=ConcurrentScanAndUpdateException.class)
    public void updatedOrderInvalidatesItemScan() throws InvalidOperationException {
        ListRowOutput output = new ListRowOutput();
        CursorId cursorId;
        try {
            cursorId = dml().openCursor(session(), aisGeneration(), scanAllRequest(iId));
            updateRow(row(oId, 2, 2), row(oId, 2, 3));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        try {
            dml().scanSome(session(), cursorId, output);
        } catch (ConcurrentScanAndUpdateException e) {
            assertEquals("rows scanned", 0, output.getRows().size());
            throw e;
        } finally {
            dml().closeCursor(session(),  cursorId);
        }
    }

    @Test(expected=ConcurrentScanAndUpdateException.class)
    public void updatedCustomerInvalidatesItemScan() throws InvalidOperationException {
        ListRowOutput output = new ListRowOutput();
        CursorId cursorId;
        try {
            cursorId = dml().openCursor(session(), aisGeneration(), scanAllRequest(iId));
            updateRow(row(cId, 3), row(cId, 4));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        try {
            dml().scanSome(session(), cursorId, output);
        } catch (ConcurrentScanAndUpdateException e) {
            assertEquals("rows scanned", 0, output.getRows().size());
            throw e;
        } finally {
            dml().closeCursor(session(),  cursorId);
        }
    }
}
