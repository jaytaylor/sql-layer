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

package com.foundationdb.server.test.costmodel;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.error.InvalidOperationException;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;

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
        schema = new Schema(ais());
        pRowType = schema.tableRowType(table(p));
        cRowType = schema.tableRowType(table(c));
        dRowType = schema.tableRowType(table(d));
        group = group(p);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
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
        Operator plan = map_NestedLoops(timeSetupOuter, timeSetupInner, 
                                        0, pipelineMap(), 1);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
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
    private TableRowType pRowType;
    private TableRowType cRowType;
    private TableRowType dRowType;
    private Group group;
}
