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

import static com.akiban.qp.operator.API.*;

public class TreeScanCT extends CostModelBase
{
    public static void main(String[] args) throws Exception
    {
        new TreeScanCT().run();
    }

    private void run() throws Exception
    {
        startTestServices();
        run(CostModelColumn.intColumn("x"));
        run(CostModelColumn.varcharColumnGoodPrefixCompression("x", 10));
        run(CostModelColumn.varcharColumnGoodPrefixCompression("x", 100));
        run(CostModelColumn.varcharColumnGoodPrefixCompression("x", 1000));
        run(CostModelColumn.varcharColumnBadPrefixCompression("x", 10));
        run(CostModelColumn.varcharColumnBadPrefixCompression("x", 100));
        run(CostModelColumn.varcharColumnBadPrefixCompression("x", 1000));
        stopTestServices();
    }

    private void run(CostModelColumn indexedColumn)
    {
        this.indexedColumn = indexedColumn;
        createSchema();
        populateDB(ROWS);
        run(WARMUP_RUNS, 1, null);
        String label = indexedColumn.description();
        run(MEASURED_RUNS, 1000, label);
        run(MEASURED_RUNS, 500, label);
        run(MEASURED_RUNS, 250, label);
        run(MEASURED_RUNS, 100, label);
        run(MEASURED_RUNS, 50, label);
        run(MEASURED_RUNS, 25, label);
        run(MEASURED_RUNS, 10, label);
        run(MEASURED_RUNS, 9, label);
        run(MEASURED_RUNS, 8, label);
        run(MEASURED_RUNS, 7, label);
        run(MEASURED_RUNS, 6, label);
        run(MEASURED_RUNS, 5, label);
        run(MEASURED_RUNS, 4, label);
        run(MEASURED_RUNS, 3, label);
        run(MEASURED_RUNS, 2, label);
        run(MEASURED_RUNS, 1, label);
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String tableName = newTableName();
        t = createTable(schemaName, tableName,
                        "id int not null",
                        indexedColumn.declaration(),
                        "primary key(id)");
        createIndex(schemaName, tableName, "idx", indexedColumn.name());
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, indexedColumn.name());
        group = groupTable(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void populateDB(int rows)
    {
        for (int id = 0; id < rows; id++) {
            dml().writeRow(session(), createNewRow(t, id, indexedColumn.valueFor(id)));
        }
        start = rows / 2;
    }

    private void run(int runs, int sequentialAccessesPerRandom, String label)
    {
        IndexBound lo = new IndexBound(row(idxRowType, indexedColumn.valueFor(start)),
                                       new SetColumnSelector(0));
        IndexBound hi = new IndexBound(row(idxRowType, indexedColumn.valueFor(Integer.MAX_VALUE)),
                                       new SetColumnSelector(0));
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
            double averageUsec = (end - start) / (1000.0 * runs * sequentialAccessesPerRandom);
            System.out.println(String.format("%s - %s:  %s usec/row", label, sequentialAccessesPerRandom, averageUsec));
        }
    }

    private static final int ROWS = 2000000; // Should create 3-level trees
    private static final int WARMUP_RUNS = 20000;
    private static final int MEASURED_RUNS = 10000;

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private GroupTable group;
    private int start;
    private CostModelColumn indexedColumn;
}
