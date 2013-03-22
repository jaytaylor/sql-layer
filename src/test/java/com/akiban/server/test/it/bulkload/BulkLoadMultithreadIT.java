
package com.akiban.server.test.it.bulkload;

import com.akiban.sql.pg.PostgresServerITBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
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
        final int THREADS = 5;
        int RUNTIME_MS = 250;

        sql("create table c(id int not null primary key)");
        sql("set bulkload='true'");

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger id = new AtomicInteger(1);
        class Inserter extends Thread {
            @Override
            public void run() {
                while (running.get()) {
                    try {
                        sql("insert into c values (" + id.incrementAndGet() + ")");
                    }
                    catch (Throwable e) {
                        exception = e;
                        return;
                    }
                }
            }

            Inserter(int threadId) {
                this.threadId = threadId;
            }

            private final int threadId;
            private Throwable exception;
        }
        List<Inserter> inserters = new ArrayList<>();
        for (int i = 0; i < THREADS; ++i) {
            Inserter inserter = new Inserter(i);
            inserters.add(inserter);
            inserter.start();
        }
        Thread.sleep(RUNTIME_MS);
        running.set(false);

        List<Throwable> exceptionsThrown = new ArrayList<>();
        for (Inserter inserter : inserters) {
            inserter.join();
            if (inserter.exception != null) {
                System.err.println("Error at inserter " + inserter.threadId);
                inserter.exception.printStackTrace();
                exceptionsThrown.add(inserter.exception);
            }
        }
        if (!exceptionsThrown.isEmpty())
            throw new RuntimeException("exceptions thrown: " + exceptionsThrown);
        sql("set bulkload='false'");
        List<List<?>> results = sql("select * from c");

        assertEquals("results count", 0, results.size());
    }


}
