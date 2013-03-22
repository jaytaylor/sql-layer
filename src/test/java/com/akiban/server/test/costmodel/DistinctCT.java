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

package com.akiban.server.test.costmodel;

import com.akiban.ais.model.Group;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import com.akiban.server.test.ExpressionGenerators;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.akiban.qp.operator.API.*;

public class DistinctCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDBBestCase(ROWS);
        for (int fields = 1; fields <= 5; fields++) {
            run(fields, "best case");
        }
        populateDBWorstCase(ROWS);
        for (int fields = 1; fields <= 5; fields++) {
            run(fields, "worst case");
        }
    }

    private void run(int fields, String label)
    {
        sort(fields, WARMUP_RUNS, null);
        sort(fields, MEASUREMENT_RUNS, label);
    }

    private void createSchema()
    {
        t = createTable(
            schemaName(), newTableName(),
            "c1 int",
            "c2 int",
            "c3 int",
            "c4 int",
            "c5 int");
        group = group(t);
        schema = new Schema(ais());
        tRowType = schema.userTableRowType(userTable(t));
        adapter = persistitAdapter(schema);
        queryContext = queryContext((PersistitAdapter) adapter);
    }

    private void populateDBBestCase(int rows)
    {
        // First column unique
        for (int i = 0; i < rows; i++) {
            NewRow row =
                createNewRow(t,
                             i,
                             0,
                             0,
                             0,
                             0);
            dml().writeRow(session(), row);
        }
    }

    private void populateDBWorstCase(int rows)
    {
        // ALl columns have the same value in all rows. Have to check all columns to determine distinctness.
        for (int id = 0; id < rows; id++) {
            NewRow row =
                createNewRow(t,
                             id,
                             0,
                             0,
                             0,
                             0,
                             0);
            dml().writeRow(session(), row);
        }
    }

    private void sort(int fields, int runs, String label)
    {
        List<ExpressionGenerator> projectFields = new ArrayList<>();
        for (int f = 0; f < fields; f++) {
            projectFields.add(ExpressionGenerators.field(tRowType, f));
        }
        Operator setup =
            project_Default(
                groupScan_Default(group),
                tRowType,
                projectFields);
        TimeOperator timeSetup = new TimeOperator(setup);
        RowType inputRowType = setup.rowType();
        Operator distinct = distinct_Partial(setup, inputRowType);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(distinct, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long distinctNsec = stop - start - timeSetup.elapsedNsec();
        if (label != null) {
            // Report the difference
            double averageUsecPerRow = distinctNsec / (1000.0 * runs * ROWS);
            System.out.println(String.format("%s, %s fields: %s usec/row",
                                             label, fields, averageUsecPerRow));
        }
    }

    private static final int ROWS = 1000;
    private static final int WARMUP_RUNS = 100;
    private static final int MEASUREMENT_RUNS = 100;

    private int t;
    private Group group;
    private Schema schema;
    private RowType tRowType;
    private StoreAdapter adapter;
}
