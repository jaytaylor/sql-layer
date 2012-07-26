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

package com.akiban.server.test.it.keyupdate;

import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.EnumSet;
import java.util.List;

import static org.junit.Assert.*;

public final class UniqueKeyUpdateIT extends ITBase {
    @Test
    public void oneColumn() throws InvalidOperationException {
        final String tableName = "t1";
        final String schemaName = "s1";
        final int tableId;
        try {
            tableId = createTable(schemaName, tableName, "cid int not null primary key", "u1 int", "UNIQUE(u1)");
            writeRows(
                    createNewRow(tableId, 11, 21),
                    createNewRow(tableId, 12, 22)
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        NewRow original = createNewRow(tableId, 12L, 22L);
        NewRow updated = createNewRow(tableId, 12L, 21L);
        try {
            dml().updateRow(session(), original, updated, ConstantColumnSelector.ALL_ON);
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }

        expectFullRows(
                tableId,
                createNewRow(tableId, 11L, 21L),
                createNewRow(tableId, 12L, 22L)
        );
        ScanRequest scanByU1 = new ScanAllRequest(
                tableId,
                set(0, 1),
                indexId(schemaName, tableName, "u1"),
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
        );
        expectRows(
                scanByU1,
                createNewRow(tableId, 11L, 21L),
                createNewRow(tableId, 12L, 22L)
        );
    }

    @Test
    public void oneColumnHKeyEquivalent() throws InvalidOperationException {
        final String tableName = "t1";
        final String schemaName = "s1";
        final int tableId;
        try {
            tableId = createTable(schemaName, tableName, "cid int not null primary key", "UNIQUE(cid)");
            writeRows(
                    createNewRow(tableId, 11),
                    createNewRow(tableId, 12)
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        NewRow original = createNewRow(tableId, 12L);
        NewRow updated = createNewRow(tableId, 11L);
        try {
            dml().updateRow(session(), original, updated, ConstantColumnSelector.ALL_ON);
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }

        expectFullRows(
                tableId,
                createNewRow(tableId, 11L),
                createNewRow(tableId, 12L)
        );
        ScanRequest scanByCid = new ScanAllRequest(
                tableId,
                set(0, 1),
                indexId(schemaName, tableName, "cid"),
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
        );
        expectRows(
                scanByCid,
                createNewRow(tableId, 11L),
                createNewRow(tableId, 12L)
        );
    }

    @Test
    public void oneColumnNoPK() throws InvalidOperationException {
        final String tableName = "t1";
        final String schemaName = "s1";

        final int tableId;
        final ScanRequest scanByCid;
        final NewRow original;
        final NewRow updated;

        try {
            tableId = createTable(schemaName, tableName, "cid int", "UNIQUE(cid)");
            writeRows(
                    createNewRow(tableId, 1, 0L),
                    createNewRow(tableId, 2, 0L)
            );
            scanByCid = new ScanAllRequest(
                    tableId,
                    set(0),
                    indexId(schemaName, tableName, "cid"),
                    EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
            );
            List<NewRow> scan = scanAll(scanByCid);
            assertEquals("scan size", 2, scan.size());
            assertEquals("scan[0] size", 1, scan.get(0).getFields().size());
            assertEquals("scan[0][0]", 1L, scan.get(0).get(0));
            assertEquals("scan[1] size", 1, scan.get(0).getFields().size());
            assertEquals("scan[1][0]", 2L, scan.get(1).get(0));

            original = scan.get(0); // (1)
            updated = new NiceRow(tableId, store());
            updated.put(0, scan.get(1).get(0)); // (2)
            original.put(1, 1L); // (1, 1)
            updated.put(1, 1L); // (2, 1)

        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        try {
            dml().updateRow(session(), original, updated, new SetColumnSelector(0));
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }
        expectRows(
                scanByCid,
                createNewRow(tableId, 1L),
                createNewRow(tableId, 2L)
        );
    }

    @Test
    public void twoColumns() throws InvalidOperationException{

        final String tableName = "t1";
        final String schemaName = "s1";
        final int tableId;
        final ScanRequest scanByU1;
        try {
            tableId = createTable(schemaName, tableName, "cid int not null primary key", "u1 int", "u2 int", "UNIQUE(u1,u2)");
            scanByU1 = new ScanAllRequest(
                    tableId,
                    set(0, 1, 2),
                    indexId(schemaName, tableName, "u1"),
                    EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
            );
            writeRows(
                    createNewRow(tableId, 11L, 21L, 31L),
                    createNewRow(tableId, 12L, 22L, 32L),
                    createNewRow(tableId, 13L, 20L, 33L)
            );
            expectRows(
                    scanByU1,
                    createNewRow(tableId, 13L, 20L, 33L),
                    createNewRow(tableId, 11L, 21L, 31L),
                    createNewRow(tableId, 12L, 22L, 32L)
            );

            // update such that there's a similarity in u1
            dml().updateRow(
                    session(),
                    createNewRow(tableId, 12L, 22L, 32L),
                    createNewRow(tableId, 12L, 21L, 32L),
                    ConstantColumnSelector.ALL_ON
            );
            expectFullRows(
                    tableId,
                    createNewRow(tableId, 11L, 21L, 31L),
                    createNewRow(tableId, 12L, 21L, 32L),
                    createNewRow(tableId, 13L, 20L, 33L)
            );
            expectRows(
                    scanByU1,
                    createNewRow(tableId, 13L, 20L, 33L),
                    createNewRow(tableId, 11L, 21L, 31L),
                    createNewRow(tableId, 12L, 21L, 32L)
            );

            // update such that there's a similarity in u2
            dml().updateRow(
                    session(),
                    createNewRow(tableId, 12L, 21L, 32L),
                    createNewRow(tableId, 12L, 21L, 33L),
                    ConstantColumnSelector.ALL_ON
            );
            expectFullRows(
                    tableId,
                    createNewRow(tableId, 11L, 21L, 31L),
                    createNewRow(tableId, 12L, 21L, 33L),
                    createNewRow(tableId, 13L, 20L, 33L)
            );
            expectRows(
                    scanByU1,
                    createNewRow(tableId, 13L, 20L, 33L),
                    createNewRow(tableId, 11L, 21L, 31L),
                    createNewRow(tableId, 12L, 21L, 33L)
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        try {
            dml().updateRow(
                    session(),
                    createNewRow(tableId, 12L, 21L, 33L),
                    createNewRow(tableId, 12L, 21L, 31L),
                    ConstantColumnSelector.ALL_ON
            );
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }

        expectFullRows(
                tableId,
                createNewRow(tableId, 11L, 21L, 31L),
                createNewRow(tableId, 12L, 21L, 33L),
                createNewRow(tableId, 13L, 20L, 33L)
        );
        expectRows(
                scanByU1,
                createNewRow(tableId, 13L, 20L, 33L),
                createNewRow(tableId, 11L, 21L, 31L),
                createNewRow(tableId, 12L, 21L, 33L)
        );
    }

    @Test
    public void nullsAreNotDuplicates() throws InvalidOperationException {
        final String tableName = "t1";
        final String schemaName = "s1";
        final int tableId;
        try {
            tableId = createTable(schemaName, tableName, "cid int not null primary key", "u1 int NULL", "UNIQUE(u1)");
            writeRows(
                    createNewRow(tableId, 11, null),
                    createNewRow(tableId, 12, 22)
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        NewRow original = createNewRow(tableId, 12L, 22L);
        NewRow updated = createNewRow(tableId, 12L, null);
        dml().updateRow(session(), original, updated, ConstantColumnSelector.ALL_ON);

        expectFullRows(
                tableId,
                createNewRow(tableId, 11L, null),
                createNewRow(tableId, 12L, null)
        );
        ScanRequest scanByU1 = new ScanAllRequest(
                tableId,
                set(0, 1),
                indexId(schemaName, tableName, "u1"),
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
        );
        expectRows(
                scanByU1,
                createNewRow(tableId, 11L, null),
                createNewRow(tableId, 12L, null)
        );
    }

    @Test
    public void pkEnforcement() {
        String tableName = "t1";
        String schemaName = "s1";
        int tableId;
        try {
            tableId = createTable(schemaName, tableName, "cid int not null primary key, cx int");
            writeRows(
                createNewRow(tableId, 1, 1),
                createNewRow(tableId, 1, 2));
            fail();
        } catch (DuplicateKeyException e) {
            // Expected
        }
    }
}
