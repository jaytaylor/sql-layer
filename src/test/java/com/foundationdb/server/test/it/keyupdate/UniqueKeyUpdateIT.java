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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.server.api.dml.ConstantColumnSelector;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.api.dml.scan.ScanAllRequest;
import com.foundationdb.server.api.dml.scan.ScanFlag;
import com.foundationdb.server.api.dml.scan.ScanRequest;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
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
        NewRow original = createNewRow(tableId, 12, 22);
        NewRow updated = createNewRow(tableId, 12, 21);
        try {
            dml().updateRow(session(), original, updated, ConstantColumnSelector.ALL_ON);
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }

        expectFullRows(
                tableId,
                createNewRow(tableId, 11, 21),
                createNewRow(tableId, 12, 22)
        );
        ScanRequest scanByU1 = new ScanAllRequest(
                tableId,
                set(0, 1),
                indexId(schemaName, tableName, "u1"),
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
        );
        expectRows(
                scanByU1,
                createNewRow(tableId, 11, 21),
                createNewRow(tableId, 12, 22)
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
        NewRow original = createNewRow(tableId, 12);
        NewRow updated = createNewRow(tableId, 11);
        try {
            dml().updateRow(session(), original, updated, ConstantColumnSelector.ALL_ON);
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }

        expectFullRows(
                tableId,
                createNewRow(tableId, 11),
                createNewRow(tableId, 12)
        );
        ScanRequest scanByCid = new ScanAllRequest(
                tableId,
                set(0, 1),
                indexId(schemaName, tableName, "cid"),
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
        );
        expectRows(
                scanByCid,
                createNewRow(tableId, 11),
                createNewRow(tableId, 12)
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
                    createNewRow(tableId, 1),
                    createNewRow(tableId, 2)
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
            assertEquals("scan[0][0]", 1, scan.get(0).get(0));
            assertEquals("scan[1] size", 1, scan.get(0).getFields().size());
            assertEquals("scan[1][0]", 2, scan.get(1).get(0));

            original = scan.get(0); // (1)
            updated = createNewRow(tableId);
            updated.put(0, scan.get(1).get(0)); // (2)
            original.put(1, 1); // (1, 1)
            updated.put(1, 1); // (2, 1)

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
                createNewRow(tableId, 1),
                createNewRow(tableId, 2)
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
                    createNewRow(tableId, 11, 21, 31),
                    createNewRow(tableId, 12, 22, 32),
                    createNewRow(tableId, 13, 20, 33)
            );
            expectRows(
                    scanByU1,
                    createNewRow(tableId, 13, 20, 33),
                    createNewRow(tableId, 11, 21, 31),
                    createNewRow(tableId, 12, 22, 32)
            );

            // update such that there's a similarity in u1
            dml().updateRow(
                    session(),
                    createNewRow(tableId, 12, 22, 32),
                    createNewRow(tableId, 12, 21, 32),
                    ConstantColumnSelector.ALL_ON
            );
            expectFullRows(
                    tableId,
                    createNewRow(tableId, 11, 21, 31),
                    createNewRow(tableId, 12, 21, 32),
                    createNewRow(tableId, 13, 20, 33)
            );
            expectRows(
                    scanByU1,
                    createNewRow(tableId, 13, 20, 33),
                    createNewRow(tableId, 11, 21, 31),
                    createNewRow(tableId, 12, 21, 32)
            );

            // update such that there's a similarity in u2
            dml().updateRow(
                    session(),
                    createNewRow(tableId, 12, 21, 32),
                    createNewRow(tableId, 12, 21, 33),
                    ConstantColumnSelector.ALL_ON
            );
            expectFullRows(
                    tableId,
                    createNewRow(tableId, 11, 21, 31),
                    createNewRow(tableId, 12, 21, 33),
                    createNewRow(tableId, 13, 20, 33)
            );
            expectRows(
                    scanByU1,
                    createNewRow(tableId, 13, 20, 33),
                    createNewRow(tableId, 11, 21, 31),
                    createNewRow(tableId, 12, 21, 33)
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        try {
            dml().updateRow(
                    session(),
                    createNewRow(tableId, 12, 21, 33),
                    createNewRow(tableId, 12, 21, 31),
                    ConstantColumnSelector.ALL_ON
            );
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }

        expectFullRows(
                tableId,
                createNewRow(tableId, 11, 21, 31),
                createNewRow(tableId, 12, 21, 33),
                createNewRow(tableId, 13, 20, 33)
        );
        expectRows(
                scanByU1,
                createNewRow(tableId, 13, 20, 33),
                createNewRow(tableId, 11, 21, 31),
                createNewRow(tableId, 12, 21, 33)
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
        NewRow original = createNewRow(tableId, 12, 22);
        NewRow updated = createNewRow(tableId, 12, null);
        dml().updateRow(session(), original, updated, ConstantColumnSelector.ALL_ON);

        expectFullRows(
                tableId,
                createNewRow(tableId, 11, null),
                createNewRow(tableId, 12, null)
        );
        ScanRequest scanByU1 = new ScanAllRequest(
                tableId,
                set(0, 1),
                indexId(schemaName, tableName, "u1"),
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
        );
        expectRows(
                scanByU1,
                createNewRow(tableId, 11, null),
                createNewRow(tableId, 12, null)
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
