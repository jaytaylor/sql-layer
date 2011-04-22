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

package com.akiban.server.test.it.keyupdate;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.DuplicateKeyException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.*;

public final class UniqueKeyUpdateIT extends ITBase {
    @Test
    public void updateBreaksUniqueness() throws InvalidOperationException {
        final String tableName = "t1";
        final String schemaName = "s1";
        final int tableId;
        try {
            tableId = createTable(schemaName, tableName, "cid int key", "u1 int", "UNIQUE(u1)");
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
            dml().updateRow(session(), original, updated, ALL_COLUMNS);
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
}
