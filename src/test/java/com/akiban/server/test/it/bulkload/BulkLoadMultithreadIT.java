/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
