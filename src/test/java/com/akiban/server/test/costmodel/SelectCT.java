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
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.expression.std.Expressions;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;

public class SelectCT extends CostModelBase
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
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        group = groupTable(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
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
        Operator select = select_HKeyOrdered(scan, tRowType, Expressions.literal(true));
        long start;
        long stop;
        // Measure time for group scan
        start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(scan, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        stop = System.nanoTime();
        long groupScanNsec = stop - start;
        // Measure time for group scan and select
        start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(select, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        stop = System.nanoTime();
        long selectNsec = stop - start;
        if (report) {
            // Report the difference
            long selectOnlyNsec = selectNsec - groupScanNsec;
            double averageUsecPerRow = selectOnlyNsec / (1000.0 * runs * ROWS);
            System.out.println(String.format("%s usec/row", averageUsecPerRow));
        }
    }

    private static final int ROWS = 100;
    private static final int WARMUP_RUNS = 20000;
    private static final int MEASURED_RUNS = 10000;

    private int t;
    private RowType tRowType;
    private GroupTable group;
}
