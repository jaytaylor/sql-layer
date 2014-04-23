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

package com.foundationdb.server.test.it.bugs.bug1112940;

import com.foundationdb.server.TableStatus;
import com.foundationdb.server.api.dml.ConstantColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * The cost estimator uses the live value from the row count accumulator
 * as it is much cheaper than a (transactional) snapshot value. However, the
 * live value is not adjusted if a modification is aborted.
 *
 * For no particular reason, the row count was bumped before we did any
 * uniqueness checks in the write row processing. This caused an increasingly
 * inaccurate gap for replication streams that use ON DUPLICATION UPDATE
 * processing, which the server sees as INSERT, (error), UPDATE
 */
public class AccumLiveValueAfterAbortIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final int ROW_COUNT = 2;
    private static final int UPDATE_COUNT = 10;
    private static enum Op { ON_DUP_KEY_UPDATE, REPLACE }

    private int tid = -1;

    private void createAndLoad() {
        tid = createTable(SCHEMA, TABLE, "id int not null primary key, x int");
        for(int i = 0; i < ROW_COUNT; ++i) {
            writeRow(tid, i, 0);
        }
        expectRowCount(tid, ROW_COUNT);
    }

    private void postCheck() {
        expectRowCount(tid, ROW_COUNT);
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                // Approximate count doesn't have to match in general, but there
                // is no reason for it to be off in these simple scenarios
                TableStatus tableStatus = getTable(tid).rowDef().getTableStatus();
                assertEquals("ApproximateRowCount", ROW_COUNT, tableStatus.getApproximateRowCount(session()));
            }
        });
    }

    private void insertAs(Op op, int id, int x) {
        NewRow newRow = createNewRow(tid, id, x);
        try {
            dml().writeRow(session(), newRow);
            fail("Expected DuplicateKeyException");
        } catch(DuplicateKeyException e) {
            // Expected
        }

        NewRow oldRow = createNewRow(tid, id, x - 1);
        if(op == Op.ON_DUP_KEY_UPDATE) {
            dml().updateRow(session(), oldRow, newRow, ConstantColumnSelector.ALL_ON);
        } else if(op == Op.REPLACE) {
            dml().deleteRow(session(), oldRow, false);
            dml().writeRow(session(), newRow);
        } else {
            fail("Unknown op: " + op);
        }
    }

    private void testOp(Op op) {
        createAndLoad();
        final int UPDATE_ID = 1;
        for(int i = 1; i <= UPDATE_COUNT; ++i) {
            insertAs(op, UPDATE_ID, i);
        }
        postCheck();
    }

    @Test
    public void onDuplicateKeyUpdate() {
        testOp(Op.ON_DUP_KEY_UPDATE);
    }

    @Test
    public void replace() {
        testOp(Op.REPLACE);
    }
}
