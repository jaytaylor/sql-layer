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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.format.PersistitStorageDescription;

import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PersistitStoreSchemaManagerIT extends PersistitStoreSchemaManagerITBase {
    private final static String SCHEMA = "my_schema";
    private final static String T1_NAME = "t1";
    private final static String T1_DDL = "id int NOT NULL, PRIMARY KEY(id)";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, T1_NAME);
    private static final int ROW_COUNT = 10;

    private int tid;
    private NewRow[] rows = new NewRow[ROW_COUNT];

    private void createAndLoad() {
        tid = createTable(SCHEMA, T1_NAME, T1_DDL);
        for(int i = 0; i < ROW_COUNT; ++i) {
            rows[i] = createNewRow(tid, i+1);
        }
        writeRows(rows);
    }

    @Test
    public void groupAndIndexTreeDelayedRemoval() throws Exception {
        createAndLoad();

        PersistitStorageDescription groupStorage = (PersistitStorageDescription)getTable(tid).getGroup().getStorageDescription();
        PersistitStorageDescription pkStorage = (PersistitStorageDescription)getTable(tid).getPrimaryKey().getIndex().getStorageDescription();
        String groupTreeName = groupStorage.getTreeName();
        String pkTreeName = pkStorage.getTreeName();
        Set<String> treeNames = pssm.getTreeNames(session());
        assertEquals("Group tree is in set before drop", true, treeNames.contains(groupTreeName));
        assertEquals("PK tree is in set before drop", true, treeNames.contains(pkTreeName));

        ddl().dropTable(session(), TABLE_NAME);

        treeNames = pssm.getTreeNames(session());
        assertEquals("Group tree is in set after drop", true, treeNames.contains(groupTreeName));
        assertEquals("PK tree is in set after drop", true, treeNames.contains(pkTreeName));

        safeRestart();

        treeNames = pssm.getTreeNames(session());
        assertEquals("Group tree is in set after restart", false, treeNames.contains(groupTreeName));
        assertEquals("PK tree is in set after restart", false, treeNames.contains(pkTreeName));
        assertEquals("Group tree exist after restart", false, store().treeExists(session(), groupStorage));
        assertEquals("PK tree exists after restart", false, store().treeExists(session(), pkStorage));
    }

    @Test
    public void createDropCreateRestart() throws Exception {
        createAndLoad();
        expectFullRows(tid, rows);
        ddl().dropTable(session(), TABLE_NAME);

        // Make sure second table gets new trees that don't get removed on restart
        createAndLoad();
        expectFullRows(tid, rows);
        safeRestart();
        expectFullRows(tid, rows);
    }

    @Test
    public void delayedTreeRemovalRollbackSafe() throws Exception {
        final String EX_MSG = "Intentional";
        createAndLoad();

        // This is a bit of a hack. Makes an impl assumption:
        // Set up a hook for the end of the DDL transaction to cause a failure.

        final TransactionService.Callback preCommitCB = new TransactionService.Callback() {
            @Override
            public void run(Session session, long timestamp) {
                throw new RuntimeException(EX_MSG);
            }
        };
        txnService().addCallbackOnInactive(session(), TransactionService.CallbackType.PRE_COMMIT, preCommitCB);

        try {
            ddl().dropTable(session(), TABLE_NAME);
            fail("Expected exception");
        } catch(RuntimeException e) {
            assertEquals("Correct exception (message)", EX_MSG, e.getMessage());
        }

        safeRestart();
        expectFullRows(tid, rows);
    }

    @Test
    public void aisCanBeReloaded() {
        createAndLoad();
        pssm.clearAISMap();
        expectFullRows(tid, rows);
    }

    @Test
    public void aisMapCleanup() {
        final int COUNT = 10;
        // Create a number of versions
        for(int i = 0; i < COUNT; ++i) {
            createTable(SCHEMA, T1_NAME+i, T1_DDL);
        }
        // Should be fully cleared after queue is cleared
        pssm.waitForQueueToEmpty(5000);
        assertEquals("AIS map size", 1, pssm.getAISMapSize());
        pssm.clearUnreferencedAISMap();
        assertEquals("AIS map size after clearing", 1, pssm.getAISMapSize());
    }

    @Test
    public void clearUnreferencedAndOpenTransaction() throws Exception {
        final int expectedTableCount = ais().getTables().size();
        createTable(SCHEMA, T1_NAME+1, T1_DDL);
        createTable(SCHEMA, T1_NAME+2, T1_DDL);

        // Construct this sequence:
        // Session 1: CREATE t1,t2                  BEGIN,CREATE t3,getAIS(),cleanup(),COMMIT
        // Session 2:               BEGIN,getAIS()                                             COMMIT
        CyclicBarrier b1 = new CyclicBarrier(2);
        CyclicBarrier b2 = new CyclicBarrier(2);
        Thread thread2 = new Thread(new AISReader(b1, b2, expectedTableCount + 2), "TestThread2");
        thread2.start();
        b1.await();
        createTable(SCHEMA, T1_NAME + 3, T1_DDL);
        txnService().beginTransaction(session());
        try {
            AkibanInformationSchema ais = ddl().getAIS(session());
            assertEquals("Table count after creates", expectedTableCount + 3, ais.getTables().size());
            pssm.clearUnreferencedAISMap();
            assertEquals("AIS map size after clearing", 2, pssm.getAISMapSize());
        } finally {
            txnService().commitTransaction(session());
        }
        b2.await();
        thread2.join();
    }


    private class AISReader implements Runnable {
        private final CyclicBarrier b1, b2;
        private final int tableCount;

        public AISReader(CyclicBarrier b1, CyclicBarrier b2, int expectedCount) {
            this.b1 = b1;
            this.b2 = b2;
            this.tableCount = expectedCount;
        }

        @Override
        public void run() {
            Session session = createNewSession();
            try {
                txnService().beginTransaction(session);
                AkibanInformationSchema ais = ddl().getAIS(session);
                b1.await();
                assertEquals("Table count (session 2)", tableCount, ais.getTables().size());
                b2.await();
                txnService().commitTransaction(session);
            } catch(Exception e) {
                throw new RuntimeException(e);
            } finally {
                txnService().rollbackTransactionIfOpen(session);
                session.close();
            }
        }
    }
}
