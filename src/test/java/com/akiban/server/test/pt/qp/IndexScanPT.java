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

package com.akiban.server.test.pt.qp;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.util.tap.Tap;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static com.akiban.qp.operator.API.*;

public class IndexScanPT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        t = createTable(
            "schema", "t",
            "id int not null key",
            "x int",
            "index(x)");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "x");
        group = groupTable(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    @Test
    public void profileGroupScan()
    {
        Tap.setEnabled(".*", false);
        populateDB(ROWS);
        run(null, WARMUP_RUNS, 1);
        run("0", MEASURED_RUNS, 1);
        run("1", MEASURED_RUNS, 2);
        run("2", MEASURED_RUNS, 3);
        run("3", MEASURED_RUNS, 4);
        run("4", MEASURED_RUNS, 5);
        run("5", MEASURED_RUNS, 6);
        run("6", MEASURED_RUNS, 7);
        run("7", MEASURED_RUNS, 8);
        run("8", MEASURED_RUNS, 9);
        run("9", MEASURED_RUNS, 10);
        run("10", MEASURED_RUNS, 11);
        run("25", MEASURED_RUNS, 26);
        run("50", MEASURED_RUNS, 51);
        run("100", MEASURED_RUNS, 101);
        run("250", MEASURED_RUNS, 251);
        run("500", MEASURED_RUNS, 501);
        run("1000", MEASURED_RUNS, 1001);
    }

    private void run(String label, int runs, int sequentialAccessesPerRandom)
    {
        IndexBound lo = new IndexBound(row(idxRowType, Integer.MAX_VALUE / 2), new SetColumnSelector(0));
        IndexBound hi = new IndexBound(row(idxRowType, Integer.MAX_VALUE), new SetColumnSelector(0));
        Ordering ordering = new Ordering();
        ordering.append(new FieldExpression(idxRowType, 0), true);
        IndexKeyRange keyRange = IndexKeyRange.bounded(idxRowType, lo, true, hi, true);
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            for (int s = 0; s < sequentialAccessesPerRandom; s++) {
                Row row = cursor.next();
                assert row != null;
            }
            cursor.close();
        }
        long end = System.nanoTime();
        if (label != null) {
            double averageMsec = (double) (end - start) / (1000 * runs);
            System.out.println(String.format("%s:  %s usec", label, averageMsec));
        }
    }

    protected void populateDB(int rows)
    {
        for (int id = 0; id < rows; id++) {
            int x = random.nextInt();
            dml().writeRow(session(), createNewRow(t, id, x));
        }
    }

    private static final int ROWS = 50000;
    private static final int WARMUP_RUNS = 20000;
    private static final int MEASURED_RUNS = 10000;

    private final Random random = new Random();
    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private GroupTable group;
}
