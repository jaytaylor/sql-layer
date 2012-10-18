/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.costmodel;

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
import static com.akiban.server.test.ExpressionGenerators.field;

public class HKeyUnionCT extends CostModelBase
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
        Operator union =
            hKeyUnion_Ordered(
                timeLeftSetup,
                timeRightSetup,
                indexRowType,
                indexRowType,
                5,
                5,
                5,
                tRowType);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(union, queryContext);
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
    private Schema schema;
    private UserTableRowType tRowType;
    private IndexRowType indexRowType;
    private StoreAdapter adapter;
}
