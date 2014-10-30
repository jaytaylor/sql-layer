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

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ConstantColumnSelector;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;
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
                    row(tableId, 11, 21),
                    row(tableId, 12, 22)
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        Row original = row(tableId, 12, 22);
        Row updated = row(tableId, 12, 21);
        try {
            updateRow(original, updated);
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }

        expectRows(
                tableId,
                row(tableId, 11, 21),
                row(tableId, 12, 22)
        );
        Index index = getTable(tableId).getIndex("u1");
        expectRows(
                index,
                row(index, 21, 11),
                row(index, 22, 12)
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
                    row(tableId, 11),
                    row(tableId, 12)
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        Row original = row(tableId, 12);
        Row updated = row(tableId, 11);
        try {
            updateRow(original, updated);
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }

        expectRows(
                tableId,
                row(tableId, 11),
                row(tableId, 12)
        );
        Index index = getTable(tableId).getIndex("cid");
        expectRows(
                index,
                row(index, 11),
                row(index, 12)
        );
    }

    @Test
    public void oneColumnNoPK() throws InvalidOperationException {
        final String tableName = "t1";
        final String schemaName = "s1";

        final int tableId;
        final Index index;
        final Row original;
        final Row updated;

        try {
            tableId = createTable(schemaName, tableName, "cid int", "UNIQUE(cid)");
            writeRows(
                    row(tableId, 1),
                    row(tableId, 2)
            );
            index = getTable(tableId).getIndex("cid");
            List<Row> scan = scanAllIndex(index);
            assertEquals("scan size", 2, scan.size());
            assertEquals("scan[0] size", 2, scan.get(0).rowType().nFields());
            assertEquals("scan[0][0]", 1, scan.get(0).value(0).getInt32());
            assertEquals("scan[1] size", 2, scan.get(1).rowType().nFields());
            assertEquals("scan[1][0]", 2, scan.get(1).value(0).getInt32());

            Row irow = scan.get(0);
            original = row(tableId, 1, irow.value(1).getInt64());
            updated = row(tableId, 2, irow.value(1).getInt64());

        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        try {
            updateRow(original, updated);
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }
        expectRowsSkipInternal(
                index,
                row(index, 1),
                row(index, 2)
        );
    }

    @Test
    public void twoColumns() throws InvalidOperationException{

        final String tableName = "t1";
        final String schemaName = "s1";
        final int tableId;
        final Index index;
        try {
            tableId = createTable(schemaName, tableName, "cid int not null primary key", "u1 int", "u2 int", "UNIQUE(u1,u2)");
            index = getTable(tableId).getIndex("u1");
            writeRows(
                    row(tableId, 11, 21, 31),
                    row(tableId, 12, 22, 32),
                    row(tableId, 13, 20, 33)
            );
            expectRows(
                    index,
                    row(index, 20, 33, 13),
                    row(index, 21, 31, 11),
                    row(index, 22, 32, 12)
            );

            // update such that there's a similarity in u1
            updateRow(
                    row(tableId, 12, 22, 32),
                    row(tableId, 12, 21, 32)
            );
            expectRows(
                    tableId,
                    row(tableId, 11, 21, 31),
                    row(tableId, 12, 21, 32),
                    row(tableId, 13, 20, 33)
            );
            expectRows(
                    index,
                    row(index, 20, 33, 13),
                    row(index, 21, 31, 11),
                    row(index, 21, 32, 12)
            );

            // update such that there's a similarity in u2
            updateRow(
                    row(tableId, 12, 21, 32),
                    row(tableId, 12, 21, 33)
            );
            expectRows(
                    tableId,
                    row(tableId, 11, 21, 31),
                    row(tableId, 12, 21, 33),
                    row(tableId, 13, 20, 33)
            );
            expectRows(
                    index,
                    row(index, 20, 33, 13),
                    row(index, 21, 31, 11),
                    row(index, 21, 33, 12)
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        try {
            updateRow(
                    row(tableId, 12, 21, 33),
                    row(tableId, 12, 21, 31)
            );
            fail("expected DuplicateKeyException");
        } catch (DuplicateKeyException e) {
            // expected
        }

        expectRows(
                tableId,
                row(tableId, 11, 21, 31),
                row(tableId, 12, 21, 33),
                row(tableId, 13, 20, 33)
        );
        expectRows(
                index,
                row(index, 20, 33, 13),
                row(index, 21, 31, 11),
                row(index, 21, 33, 12)
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
                    row(tableId, 11, null),
                    row(tableId, 12, 22)
            );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
        Row original = row(tableId, 12, 22);
        Row updated = row(tableId, 12, null);
        updateRow(original, updated);

        expectRows(
                tableId,
                row(tableId, 11, null),
                row(tableId, 12, null)
        );
        Index index = getTable(tableId).getIndex("u1");
        expectRows(
                index,
                row(index, null, 11),
                row(index, null, 12)
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
                row(tableId, 1, 1),
                row(tableId, 1, 2));
            fail();
        } catch (DuplicateKeyException e) {
            // Expected
        }
    }
}
