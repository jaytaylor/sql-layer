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

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilder;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import org.junit.Test;

import java.util.List;

import static com.foundationdb.qp.operator.API.insert_Returning;
import static com.foundationdb.qp.operator.API.valuesScan_Default;

public class ConcurrentInsertMT extends MTBase
{
    public static int ROW_COUNT = 100;
    private final int TIMEOUT = 10 * 1000;

    private RowType rowType;

    private void createHiddenPK() {
        int tid = createTable("test", "t", "id INT");
        rowType = SchemaCache.globalSchema(ais()).tableRowType(tid);
    }

    private void createExplicitPK() {
        int tid = createTable("test", "t", "id INT NOT NULL PRIMARY KEY, x INT");
        rowType = SchemaCache.globalSchema(ais()).tableRowType(tid);
    }

    private void run(int threadCount) {
        ConcurrentTestBuilder builder = ConcurrentTestBuilderImpl.create();
        int perThread = ROW_COUNT / threadCount;
        for(int i = 0; i < threadCount; ++i) {
            builder.add("thread_"+i, insertCreator(rowType, 1 + i * perThread, perThread))
                   .sync("a", Stage.PRE_BEGIN);
        }
        List<MonitoredThread> threads = builder.build(this);
        ThreadHelper.runAndCheck(TIMEOUT,threads);
    }

    private static OperatorCreator insertCreator(final RowType rowType, int startID, int count) {
        final Row[] rows = new Row[count];
        for(int i = 0; i < count; ++i) {
            rows[i] = new TestRow(rowType, startID + i, -1L);
        }
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType outType = schema.tableRowType(rowType.typeId());
                return insert_Returning(valuesScan_Default(bindableRows(rows), outType));
            }
        };
    }


    @Test
    public void hiddenPKTwoThreads() {
        createHiddenPK();
        run(2);
    }

    @Test
    public void hiddenPKTenThreads() {
        createHiddenPK();
        run(10);
    }

    @Test
    public void explicitPKTwoThreads() {
        createExplicitPK();
        run(2);
    }

    @Test
    public void explicitPKTenThreads() {
        createExplicitPK();
        run(10);
    }
}
