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
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.ExpressionGenerators;

import org.junit.Test;

import java.util.Random;

import static com.foundationdb.qp.operator.API.*;

public class SortWithLimitCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB(1000);
        run(1);
        run(2);
        run(3);
        run(4);
        run(5);
    }

    private void run(int sortFields)
    {
        // Warmup
        sort(sortFields, WARMUP_RUNS, 8, false);
        // Measurements
        sort(sortFields, MEASUREMENT_RUNS, 128, true);
        sort(sortFields, MEASUREMENT_RUNS, 64, true);
        sort(sortFields, MEASUREMENT_RUNS, 32, true);
        sort(sortFields, MEASUREMENT_RUNS, 16, true);
        sort(sortFields, MEASUREMENT_RUNS, 8, true);
        sort(sortFields, MEASUREMENT_RUNS, 4, true);
        sort(sortFields, MEASUREMENT_RUNS, 2, true);
        sort(sortFields, MEASUREMENT_RUNS, 1, true);
    }

    private void createSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "a int",
            "b int",
            "c int",
            "d int",
            "e int",
            "primary key(id)");
        group = group(t);
        schema = SchemaCache.globalSchema(ais());
        tRowType = schema.tableRowType(table(t));
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    private void populateDB(int rows)
    {
        for (int id = 0; id < rows; id++) {
            writeRow(session(), row(t,
                                    id,
                                    random.nextInt(),
                                    random.nextInt(),
                                    random.nextInt(),
                                    random.nextInt(),
                                    random.nextInt()));
        }
    }

    private void sort(int sortFields,
                      int runs,
                      int rows,
                      boolean report)
    {
        Operator setup = limit_Default(groupScan_Default(group), rows);
        TimeOperator timeSetup = new TimeOperator(setup);
        Ordering ordering = ordering();
        for (int f = 0; f < sortFields; f++) {
            ordering.append(ExpressionGenerators.field(tRowType, f), true);
        }
        Operator sort = sort_InsertionLimited(timeSetup, tRowType, ordering, SortOption.PRESERVE_DUPLICATES, rows);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(sort, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long sortNsec = stop - start - timeSetup.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = sortNsec / (1000.0 * runs * rows);
            System.out.println(String.format("sort fields: %s, rows: %s: %s usec/row",
                                             sortFields,
                                             rows,
                                             averageUsecPerRow));
        }
    }

    private static final int WARMUP_RUNS = 10000;
    private static final int MEASUREMENT_RUNS = 10000;

    private final Random random = new Random();
    private int t;
    private Group group;
    private Schema schema;
    private RowType tRowType;
    private StoreAdapter adapter;
}
