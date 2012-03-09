/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.costmodel;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.error.InvalidOperationException;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;

public class MapCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        run(WARMUP_RUNS, 5, false);
        for (int innerRows : INNER_ROWS_PER_OUTER) {
            run(MEASURED_RUNS, innerRows, true);
        }
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String pTableName = newTableName();
        p = createTable(schemaName, pTableName,
                        "pid int not null",
                        "primary key(pid)");
        String cTableName = newTableName();
        c = createTable(schemaName, cTableName,
                        "cid int not null",
                        "pid int",
                        "primary key(cid)",
                        String.format("grouping foreign key(pid) references %s(pid)", pTableName));
        String dTableName = newTableName();
        d = createTable(schemaName, dTableName,
                        "did int not null",
                        "pid int",
                        "primary key(did)",
                        String.format("grouping foreign key(pid) references %s(pid)", pTableName));
        schema = new Schema(rowDefCache().ais());
        pRowType = schema.userTableRowType(userTable(p));
        cRowType = schema.userTableRowType(userTable(c));
        dRowType = schema.userTableRowType(userTable(d));
        group = groupTable(p);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void populateDB()
    {
        for (int r = 0; r < OUTER_ROWS; r++) {
            dml().writeRow(session(), createNewRow(p, r));
        }
    }
    
    private void run(int runs, int innerRows, boolean report)
    {
        Operator setupOuter = groupScan_Default(group);
        TimeOperator timeSetupOuter = new TimeOperator(setupOuter);
        Operator setupInner = limit_Default(groupScan_Default(group), innerRows);
        TimeOperator timeSetupInner = new TimeOperator(setupInner);
        Operator plan = map_NestedLoops(timeSetupOuter, timeSetupInner, 0);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long mapNsec = stop - start - timeSetupInner.elapsedNsec() - timeSetupOuter.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = mapNsec / (1000.0 * runs * (OUTER_ROWS * (innerRows + 1)));
            System.out.println(String.format("inner/outer = %s: %s usec/row",
                                             innerRows, averageUsecPerRow));
        }
    }

    private static final int WARMUP_RUNS = 1000;
    private static final int MEASURED_RUNS = 1000;
    private static final int OUTER_ROWS = 100;
    private static final int[] INNER_ROWS_PER_OUTER = new int[]{64, 32, 16, 8, 4, 2, 1, 0};

    private int p;
    private int c;
    private int d;
    private UserTableRowType pRowType;
    private UserTableRowType cRowType;
    private UserTableRowType dRowType;
    private GroupTable group;
}
