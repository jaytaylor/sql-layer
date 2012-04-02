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

package com.akiban.server.test.it.multiscan_update;

import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.error.ConcurrentScanAndUpdateException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
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
            cursorId = dml().openCursor(session(), aisGeneration(), scanAllRequest(iId));
            dml().updateRow(
                    session(),
                    createNewRow(oId, 2, 2),
                    createNewRow(oId, 2, 3),
                    ConstantColumnSelector.ALL_ON
            );
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
            dml().updateRow(
                    session(),
                    createNewRow(cId, 3),
                    createNewRow(cId, 4),
                    ConstantColumnSelector.ALL_ON
            );
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
