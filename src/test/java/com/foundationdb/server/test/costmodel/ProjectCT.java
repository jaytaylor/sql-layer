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
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.ExpressionGenerators;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.qp.operator.API.*;

public class ProjectCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB(ROWS);
        run(WARMUP_RUNS, false);
        run(MEASURED_RUNS, true);
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String tableName = newTableName();
        t = createTable(schemaName, tableName,
                        "id int not null",
                        "primary key(id)");
        schema = new Schema(ais());
        tRowType = schema.userTableRowType(userTable(t));
        group = group(t);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB(int rows)
    {
        for (int id = 0; id < rows; id++) {
            dml().writeRow(session(), createNewRow(t, id));
        }
    }

    private void run(int runs, boolean report)
    {
        Operator scan = groupScan_Default(group);
        TimeOperator timeScan = new TimeOperator(scan);
        Operator project = project_Default(timeScan, tRowType, Arrays.asList(ExpressionGenerators.literal(true)));
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(project, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long projectNsec = stop - start - timeScan.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = projectNsec / (1000.0 * runs * ROWS);
            System.out.println(String.format("%s usec/row", averageUsecPerRow));
        }
    }

    private static final int ROWS = 100;
    private static final int WARMUP_RUNS = 20000;
    private static final int MEASURED_RUNS = 10000;

    private int t;
    private RowType tRowType;
    private Group group;
}
