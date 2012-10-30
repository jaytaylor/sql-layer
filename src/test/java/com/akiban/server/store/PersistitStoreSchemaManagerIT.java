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

package com.akiban.server.store;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.service.session.Session;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Semaphore;

import static com.akiban.server.store.PersistitStoreSchemaManager.SerializationType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
            rows[i] = createNewRow(tid, i+1L);
        }
        writeRows(rows);
    }


    @Test
    public void newDataSetReadAndSavedAsProtobuf() throws Exception {
        createTable(SCHEMA, T1_NAME, T1_DDL);
        assertEquals("Saved as PROTOBUF", SerializationType.PROTOBUF, pssm.getSerializationType());

        safeRestart();

        assertEquals("Saw PROTOBUF on load", SerializationType.PROTOBUF, pssm.getSerializationType());
    }

    @Test
    public void groupAndIndexTreeDelayedRemoval() throws Exception {
        createAndLoad();

        String groupTreeName = getUserTable(tid).getGroup().getTreeName();
        String pkTreeName = getUserTable(tid).getPrimaryKey().getIndex().getTreeName();
        Set<String> treeNames = pssm.getTreeNames();
        assertEquals("Group tree is in set before drop", true, treeNames.contains(groupTreeName));
        assertEquals("PK tree is in set before drop", true, treeNames.contains(pkTreeName));

        ddl().dropTable(session(), TABLE_NAME);

        treeNames = pssm.getTreeNames();
        assertEquals("Group tree is in set after drop", true, treeNames.contains(groupTreeName));
        assertEquals("PK tree is in set after drop", true, treeNames.contains(pkTreeName));

        safeRestart();

        treeNames = pssm.getTreeNames();
        assertEquals("Group tree is in set after restart", false, treeNames.contains(groupTreeName));
        assertEquals("PK tree is in set after restart", false, treeNames.contains(pkTreeName));
        assertEquals("Group tree exist after restart", false, treeService().treeExists(SCHEMA, groupTreeName));
        assertEquals("PK tree exists after restart", false, treeService().treeExists(SCHEMA, pkTreeName));
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
        createAndLoad();

        // Start a transaction for this thread so it can be aborted manually. Less invasive than hooks or similar.
        treeService().getDb().getTransaction().begin();
        ddl().dropTable(session(), TABLE_NAME);
        treeService().getDb().getTransaction().rollback();
        treeService().getDb().getTransaction().end();

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
        assertEquals("More than 1 AIS in map", true, pssm.getAISMapSize() > 1);
        pssm.clearUnreferencedAISMap();
        assertEquals("AIS map size after clearing", 1, pssm.getAISMapSize());
    }

    @Test
    public void oldestActiveCausesMapCleanup() {
        final int COUNT = 10;
        // Create a number of versions
        for(int i = 0; i < COUNT; ++i) {
            createTable(SCHEMA, T1_NAME+i, T1_DDL);
        }
        assertEquals("More than 1 AIS in map", true, pssm.getAISMapSize() > 1);
        long oldest1 = pssm.getOldestActiveAISGeneration();
        assertEquals("AIS map size after clearing", 1, pssm.getAISMapSize());
        long oldest2 = pssm.getOldestActiveAISGeneration();
        assertTrue("Oldest active changed: ", oldest1 != oldest2);
    }

    @Test
    public void clearUnreferencedAndOpenTransaction() throws BrokenBarrierException, InterruptedException {
        final int expectedTableCount = ais().getUserTables().size();
        createTable(SCHEMA, T1_NAME+1, T1_DDL);
        createTable(SCHEMA, T1_NAME+2, T1_DDL);

        // Construct this sequence:
        // Session 1: CREATE t1,t2                  BEGIN,CREATE t3,getAIS(),cleanup(),COMMIT
        // Session 2:               BEGIN,getAIS()                                             COMMIT
        Semaphore sem = new Semaphore(0);
        Thread thread2 = new Thread(new AISReader(sem, expectedTableCount + 2), "TestThread2");
        thread2.start();
        sem.acquire(); // wait for get
        createTable(SCHEMA, T1_NAME + 3, T1_DDL);
        txnService().beginTransaction(session());
        try {
            AkibanInformationSchema ais = ddl().getAIS(session());
            assertEquals("Table count after creates", expectedTableCount + 3, ais.getUserTables().size());
            pssm.clearUnreferencedAISMap();
            assertEquals("AIS map size after clearing", 2, pssm.getAISMapSize());
        } finally {
            txnService().commitTransaction(session());
        }
        sem.release(); // trigger commit
        thread2.join();
    }


    private class AISReader implements Runnable {
        private final Semaphore sem;
        private final int tableCount;

        public AISReader(Semaphore sem, int expectedCount) {
            this.sem = sem;
            this.tableCount = expectedCount;
        }

        @Override
        public void run() {
            Session session = createNewSession();
            try {
                txnService().beginTransaction(session);
                AkibanInformationSchema ais = ddl().getAIS(session);
                sem.release();
                assertEquals("Table count (session 2)", tableCount, ais.getUserTables().size());
                sem.acquire(); // Wait for cleanup/commit
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
