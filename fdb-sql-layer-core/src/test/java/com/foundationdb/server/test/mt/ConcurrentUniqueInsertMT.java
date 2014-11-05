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

package com.foundationdb.server.test.mt;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilder;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadHelper.UncaughtHandler;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConcurrentUniqueInsertMT extends MTBase
{
    private int tid;
    private RowType rowType;

    private void createNotNullUnique() {
        tid = createTable("test", "t", "id INT NOT NULL PRIMARY KEY, x INT NOT NULL, UNIQUE(x)");
        rowType = SchemaCache.globalSchema(ais()).tableRowType(tid);
    }

    private void createNullUnique() {
        tid = createTable("test", "t", "id INT NOT NULL PRIMARY KEY, x INT NULL, UNIQUE(x)");
        rowType = SchemaCache.globalSchema(ais()).tableRowType(tid);
    }

    private List<MonitoredThread> buildThreads(Integer x1, Integer x2) {
        ConcurrentTestBuilder builder = ConcurrentTestBuilderImpl.create();
        builder.add("insert_a", insertCreator(tid, testRow(rowType, 1, x1)))
               .sync("sync", Stage.POST_BEGIN)
               .add("insert_b", insertCreator(tid, testRow(rowType, 2, x2)))
               .sync("sync", Stage.POST_BEGIN);
        return builder.build(this);
    }

    private void expectSuccess(List<MonitoredThread> threads) {
        ThreadHelper.runAndCheck(threads);
    }

    private void expectOneFailure(List<MonitoredThread> threads) {
        UncaughtHandler handler = ThreadHelper.startAndJoin(threads);
        assertEquals("failure count", 1, handler.thrown.size());
        Throwable failure = handler.thrown.values().iterator().next();
        assertEquals("failure was dup", DuplicateKeyException.class, failure.getClass());
        assertEquals("saw rollback", true, threads.get(0).hadRollback() || threads.get(1).hadRollback());
    }

    @Test
    public void notNullSameValue() {
        createNotNullUnique();
        expectOneFailure(buildThreads(42, 42));
    }

    @Test
    public void notNullDifferentValue() {
        createNotNullUnique();
        expectSuccess(buildThreads(42, 52));
    }

    @Test
    public void nullBothNull() {
        createNullUnique();
        expectSuccess(buildThreads(null, null));
    }

    @Test
    public void nullSameValue() {
        createNotNullUnique();
        expectOneFailure(buildThreads(42, 42));
    }

    @Test
    public void nullDifferentValue() {
        createNotNullUnique();
        expectSuccess(buildThreads(42, 52));
    }
}
