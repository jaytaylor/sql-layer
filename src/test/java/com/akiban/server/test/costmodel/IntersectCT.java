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
import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.std.FieldExpression;
import org.junit.Test;

import java.util.Random;

import static com.akiban.qp.operator.API.*;
import static com.akiban.qp.operator.API.indexScan_Default;

public class IntersectCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        intersect(WARMUP_RUNS, LAST_KEY_COLUMN_UNIQUE, LAST_KEY_COLUMN_UNIQUE, null);
        intersect(MEASUREMENT_RUNS, FIRST_KEY_COLUMN_UNIQUE, FIRST_KEY_COLUMN_UNIQUE, "first/first");
        intersect(MEASUREMENT_RUNS, LAST_KEY_COLUMN_UNIQUE, LAST_KEY_COLUMN_UNIQUE, "last/last");
        intersect(MEASUREMENT_RUNS, FIRST_KEY_COLUMN_UNIQUE, LAST_KEY_COLUMN_UNIQUE, "first/last");
    }

    private void createSchema()
    {
        String tableName = newTableName();
        t = createTable(
            schemaName(), tableName,
            "index_key int",
            "c1 int not null",
            "c2 int not null",
            "c3 int not null",
            "c4 int not null",
            "c5 int not null",
            "primary key(c1, c2, c3, c4, c5)");
        Index index = createIndex(schemaName(), tableName, "idx", "index_key");
        group = groupTable(t);
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        indexRowType = schema.indexRowType(index);
        adapter = persistitAdapter(schema);
        queryContext = queryContext((PersistitAdapter) adapter);
    }

    private void populateDB()
    {
        // First column determines match
        NewRow row;
        for (int i = 0; i < ROWS; i++) {
            row = createNewRow(t,
                               FIRST_KEY_COLUMN_UNIQUE, // index_key
                               i, // c1
                               FIRST_KEY_COLUMN_UNIQUE, //c2
                               FIRST_KEY_COLUMN_UNIQUE, //c3
                               FIRST_KEY_COLUMN_UNIQUE, //c4
                               FIRST_KEY_COLUMN_UNIQUE); //c5
            dml().writeRow(session(), row);
            row = createNewRow(t,
                               LAST_KEY_COLUMN_UNIQUE, // index_key
                               LAST_KEY_COLUMN_UNIQUE, // c1
                               LAST_KEY_COLUMN_UNIQUE, // c2
                               LAST_KEY_COLUMN_UNIQUE, // c3
                               LAST_KEY_COLUMN_UNIQUE, // c4
                               i); // c5
            dml().writeRow(session(), row);
        }
    }

    private void intersect(int runs, int leftIndexKey, int rightIndexKey, String label)
    {
        Ordering ordering = new Ordering();
        ordering.append(new FieldExpression(indexRowType, 1), true);
        ordering.append(new FieldExpression(indexRowType, 2), true);
        ordering.append(new FieldExpression(indexRowType, 3), true);
        ordering.append(new FieldExpression(indexRowType, 4), true);
        ordering.append(new FieldExpression(indexRowType, 5), true);
        IndexBound leftBound = new IndexBound(row(indexRowType, leftIndexKey), new SetColumnSelector(0));
        IndexKeyRange leftKeyRange = IndexKeyRange.bounded(indexRowType, leftBound, true, leftBound, true);
        IndexBound rightBound = new IndexBound(row(indexRowType, rightIndexKey), new SetColumnSelector(0));
        IndexKeyRange rightKeyRange = IndexKeyRange.bounded(indexRowType, rightBound, true, rightBound, true);
        Operator leftSetup = indexScan_Default(indexRowType, leftKeyRange, ordering);
        Operator rightSetup = indexScan_Default(indexRowType, rightKeyRange, ordering);
        TimeOperator timeLeftSetup = new TimeOperator(leftSetup);
        TimeOperator timeRightSetup = new TimeOperator(rightSetup);
        Operator intersect = 
            intersect_Ordered(
                timeLeftSetup,
                timeRightSetup,
                indexRowType,
                indexRowType,
                5,
                5,
                5,
                JoinType.INNER_JOIN,
                IntersectOutputOption.OUTPUT_LEFT);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(intersect, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long intersectNsec = stop - start - timeLeftSetup.elapsedNsec() - timeRightSetup.elapsedNsec();
        if (label != null) {
            // Report the difference
            double averageUsecPerRow = intersectNsec / (1000.0 * runs * 2 * ROWS);
            System.out.println(String.format("%s: %s usec/row",
                                             label, averageUsecPerRow));
        }
    }

    private static final int ROWS = 10000;
    private static final int WARMUP_RUNS = 100;
    private static final int MEASUREMENT_RUNS = 100;
    private static final int FIRST_KEY_COLUMN_UNIQUE = 1;
    private static final int LAST_KEY_COLUMN_UNIQUE = 5;

    private final Random random = new Random();
    private int t;
    private GroupTable group;
    private Schema schema;
    private UserTableRowType tRowType;
    private IndexRowType indexRowType;
    private StoreAdapter adapter;
}
