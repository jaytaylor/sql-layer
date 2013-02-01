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

package com.akiban.server.test.it.bugs.bug1112940;

import com.akiban.ais.model.UserTable;
import com.akiban.server.TableStatus;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AccumLiveValueAfterAbortIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";

    private static final int ROW_COUNT = 2;
    private static final int UPDATE_COUNT = 10;

    /**
     * The cost estimator uses the live value from the row count
     * accumulator for making decisions as it is much cheaper
     * than a (transactional) snapshot value. However, the live
     * value is not adjusted if a modification is aborted.
     *
     * For no particular reason, the row count was bumped before
     * we did any uniqueness checks in the write row processing.
     * This caused an increasingly inaccurate gap for replication
     * streams that use ON DUPLICATION UPDATE processing, which
     * the server sees as INSERT (violation), UPDATE
     */
    @Test
    public void doTest() {
        int tid = createTable(SCHEMA, TABLE, "id int not null primary key, x int");

        for(int i = 0; i < ROW_COUNT; ++i) {
            writeRow(tid, i, 0);
        }

        expectRowCount(tid, ROW_COUNT);

        final int UPDATE_ID = 1;
        for(int i = 1; i <= UPDATE_COUNT; ++i) {
            try {
                writeRow(tid, UPDATE_ID, i);
                fail("Expected DuplicateKeyException, loop " + i);
            } catch(DuplicateKeyException e) {
                // Expected
            }

            dml().updateRow(session(),
                            createNewRow(tid, UPDATE_ID, i - 1),
                            createNewRow(tid, UPDATE_ID, i - 1),
                            ConstantColumnSelector.ALL_ON);
        }

        expectRowCount(tid, ROW_COUNT);

        // Approximate count doesn't have to match in general, but there
        // is no reason for it to be off in this simple scenario
        TableStatus tableStatus = getUserTable(tid).rowDef().getTableStatus();
        assertEquals("ApproximateRowCount", ROW_COUNT, tableStatus.getApproximateRowCount());
    }
}
