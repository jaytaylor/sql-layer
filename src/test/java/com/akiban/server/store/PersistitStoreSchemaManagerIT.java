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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

import static com.akiban.server.store.PersistitStoreSchemaManager.SerializationType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PersistitStoreSchemaManagerIT extends PersistitStoreSchemaManagerITBase {
    private static final Logger LOG = LoggerFactory.getLogger(PersistitStoreSchemaManagerIT.class.getName());

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
        // Should be fully cleared after DDL is committed (performed synchronouslyy)
        assertEquals("AIS map size", 1, pssm.getAISMapSize());
        pssm.clearUnreferencedAISMap();
        assertEquals("AIS map size after clearing", 1, pssm.getAISMapSize());
    }

    @Test
    public void clearUnreferencedAndOpenTransaction() throws BrokenBarrierException, InterruptedException {
        final int expectedTableCount = ais().getUserTables().size();
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
            assertEquals("Table count after creates", expectedTableCount + 3, ais.getUserTables().size());
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
                assertEquals("Table count (session 2)", tableCount, ais.getUserTables().size());
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
