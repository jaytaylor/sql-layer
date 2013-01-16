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

package com.akiban.server.test.it.bulkload;

import com.akiban.sql.pg.PostgresServerITBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * This should perhaps technically be an MT, but it's intended to test really simple multithreading behavior
 * which feels closer to basic functionality.
 */
public final class BulkLoadMultithreadIT extends PostgresServerITBase {
    @Test
    public void parallelLoad() throws Exception {
        sql("create table c(id int not null primary key)");
        sql("set bulkload='true'");

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicInteger id = new AtomicInteger(1);
        final AtomicBoolean running = new AtomicBoolean(true);
        class Inserter extends Thread {
            private Exception exception;
            @Override
            public void run() {
                while (running.get()) {
                    sql("insert into c values (" + id.incrementAndGet() + ")");
                    try {
                        barrier.await();
                    } catch (Exception e) {
                        exception = e;
                        break;
                    }
                }
            }
        }
        List<Inserter> inserters = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            Inserter inserter = new Inserter();
            inserter.start();
        }
        Thread.sleep(1000);
        running.set(false);
        for (Inserter inserter : inserters) {
            inserter.join();
            if (inserter.exception != null)
                throw inserter.exception;
        }
        sql("set bulkload='true'");
        List<List<?>> results = sql("select * from c");

        assertEquals("results count", 0, results.size());
    }


}
