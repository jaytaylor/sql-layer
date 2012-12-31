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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.IndexKeyVisitor;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

// Inspired by bug 985007

@Ignore
public final class KeyUpdateAcrossTransactionsIT extends ITBase
{
    @Before
    public final void before() throws Exception
    {
        testStore = new TestStore(store(), persistitStore());
    }

    @Test
    public void testUniqueViolationAcrossTransactions() throws Exception
    {
        final String TABLE_NAME = "t";
        final String SCHEMA_NAME = "s";
        final int tableId=
            createTable(SCHEMA_NAME, TABLE_NAME,
                        "id int not null primary key",
                        "u int",
                        "unique(u)");
        writeRows(createNewRow(tableId, 0, 0));
        AkibanInformationSchema ais = ddl().getAIS(session());
        UserTable table = ais.getUserTable(tableId);
        Index uIndex = null;
        for (TableIndex index : table.getIndexes()) {
            if (index.getKeyColumns().get(0).getColumn().getName().equals("u")) {
                uIndex = index;
            }
        }
        assertNotNull(uIndex);
        final Index finalUIndex = uIndex;
        CyclicBarrier barrier = new CyclicBarrier(2);
        TestThread t1 = createThread(barrier, tableId, 100, 999);
        TestThread t2 = createThread(barrier, tableId, 101, 999);
        t1.join();
        t2.join();
        final Set<Long> uniqueKeys = new HashSet<Long>();
        transactionally(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                testStore.traverse(session(),
                                   finalUIndex,
                                   new IndexKeyVisitor()
                                   {
                                       @Override
                                       protected void visit(List<?> key)
                                       {
                                           Long u = (Long) key.get(0);
                                           boolean added = uniqueKeys.add(u);
                                           assertTrue(key.toString(), added);
                                       }
                                   });
                return null;
            }
        });
    }

    public TestThread createThread(CyclicBarrier barrier, int tableId, int id, int u)
    {
        TestThread thread = new TestThread(barrier, tableId, id, u);
        thread.start();
        return thread;
    }

    private TestStore testStore;

    private class TestThread extends Thread
    {
        @Override
        public void run()
        {
            try {
                Session session = createNewSession();
                txnService().beginTransaction(session);
                try {
                    System.out.println(String.format("(%s, %s), %s: Starting", id, u, session));
                    barrier.await();
                    System.out.println(String.format("(%s, %s), %s: About to write", id, u, session));
                    dml().writeRow(session, createNewRow(tableId, id, u));
                    System.out.println(String.format("(%s, %s), %s: Wrote", id, u, session));
                    barrier.await();
                    System.out.println(String.format("(%s, %s), %s: Exiting", id, u, session));
                    txnService().commitTransaction(session);
                }
                finally {
                    txnService().rollbackTransactionIfOpen(session);
                }
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }

        public TestThread(CyclicBarrier barrier, int tableId, int id, int u)
        {
            this.barrier = barrier;
            this.tableId = tableId;
            this.id = id;
            this.u = u;
        }

        private final CyclicBarrier barrier;
        private final int tableId;
        private final int id;
        private final int u;
    }
}
