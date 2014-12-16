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

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;

import org.junit.Test;

import java.util.Random;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;

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
        schema = SchemaCache.globalSchema(ais());
        tRowType = schema.tableRowType(table(t));
        indexRowType = schema.indexRowType(index);
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    private void populateDB()
    {
        // First column determines match
        Row row;
        for (int i = 0; i < ROWS; i++) {
            row = row(t, FIRST_KEY_COLUMN_UNIQUE, // index_key
                      i, // c1
                      FIRST_KEY_COLUMN_UNIQUE, //c2
                      FIRST_KEY_COLUMN_UNIQUE, //c3
                      FIRST_KEY_COLUMN_UNIQUE, //c4
                      FIRST_KEY_COLUMN_UNIQUE); //c5
            writeRow(row);
            row = row(t, LAST_KEY_COLUMN_UNIQUE, // index_key
                      LAST_KEY_COLUMN_UNIQUE, // c1
                      LAST_KEY_COLUMN_UNIQUE, // c2
                      LAST_KEY_COLUMN_UNIQUE, // c3
                      LAST_KEY_COLUMN_UNIQUE, // c4
                      i); // c5
            writeRow(row);
        }
    }

    private void intersect(int runs, int leftIndexKey, int rightIndexKey, String label)
    {
        Ordering ordering = new Ordering();
        ordering.append(field(indexRowType, 1), true);
        ordering.append(field(indexRowType, 2), true);
        ordering.append(field(indexRowType, 3), true);
        ordering.append(field(indexRowType, 4), true);
        ordering.append(field(indexRowType, 5), true);
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
                    IntersectOption.OUTPUT_LEFT,
                    null);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(intersect, queryContext, queryBindings);
            cursor.openTopLevel();
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
    private Schema schema;
    private TableRowType tRowType;
    private IndexRowType indexRowType;
    private StoreAdapter adapter;
}
