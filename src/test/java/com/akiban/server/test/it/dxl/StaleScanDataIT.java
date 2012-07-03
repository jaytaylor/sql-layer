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

package com.akiban.server.test.it.dxl;

import com.akiban.server.api.FixedCountLimit;
import com.akiban.server.api.dml.scan.ColumnSet;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

// Inspired by bug 885697

public final class StaleScanDataIT extends ITBase
{
    @Test
    public void simpleScanLimit() throws InvalidOperationException
    {
        int t1 = createTable("schema", "t1",
                             "c1 int",
                             "c2 int",
                             "c3 int",
                             "id int not null primary key");
        int t2 = createTable("schema", "t2",
                             "id int not null primary key",
                             "c1 int");
        // Load some data
        dml().writeRow(session(), createNewRow(t1, 0, 0, 0, 0));
        dml().writeRow(session(), createNewRow(t1, 1, 1, 1, 1));
        dml().writeRow(session(), createNewRow(t2, 2, 2));
        dml().writeRow(session(), createNewRow(t2, 3, 3));
        // Start a scan on t1. Should leave a ScanData hanging around.
        ScanRequest t1ScanRequest = new ScanAllRequest(t1,
                                                       ColumnSet.ofPositions(0, 1, 2, 3),
                                                       0,
                                                       null,
                                                       new FixedCountLimit(1));
        CursorId cursorId = dml().openCursor(session(), aisGeneration(), t1ScanRequest);
        assertEquals(1, dml().getCursors(session()).size());
        // Update a t2 row. This provokes bug 885697 because:
        // - The rows passed in are LegacyRowWrappers (as opposed to NiceRows). Indexing into the RowData's RowDef
        //   with a too-high field number results in ArrayIndexOutOfBoundsException.
        // - ColumnSelector is null (as in an UpdateRowRequest), so that we get past the ColumnSelector check in
        //   BasicDMLFunctions.checkForModifiedCursors to retrieve a field from the old/new rows.
        // - There is a non-closed ScanData whose index contains columns from field positions that don't exist
        //   in the old/new rows, (set up using the t1 scan).
        dml().updateRow(session(),
                        new LegacyRowWrapper(createNewRow(t2, 2, 2).toRowData(), store()),
                        new LegacyRowWrapper(createNewRow(t2, 2, 999).toRowData(), store()),
                        null);
        dml().closeCursor(session(), cursorId);
        assertTrue(dml().getCursors(session()).isEmpty());
    }
}
