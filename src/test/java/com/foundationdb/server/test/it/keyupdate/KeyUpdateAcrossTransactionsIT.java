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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.IndexKeyVisitor;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

// Inspired by bug 985007

@Ignore
public final class KeyUpdateAcrossTransactionsIT extends ITBase
{
    @Before
    public final void before() throws Exception
    {
        testStore = new TestStore(store());
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
        writeRows(row(tableId, 0, 0));
        AkibanInformationSchema ais = ddl().getAIS(session());
        Table table = ais.getTable(tableId);
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
        final Set<Long> uniqueKeys = new HashSet<>();
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
                                   }, 
                                   -1, 0);
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
                    writeRows(session, Arrays.asList(row(session, tableId, id, u)));
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
