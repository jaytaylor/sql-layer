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

import com.akiban.ais.model.Group;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.expression.std.Expressions;
import org.junit.Test;

import java.util.Random;

import static com.akiban.qp.operator.API.*;

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
        schema = new Schema(ais());
        tRowType = schema.userTableRowType(userTable(t));
        adapter = persistitAdapter(schema);
        queryContext = queryContext((PersistitAdapter) adapter);
    }

    private void populateDB(int rows)
    {
        for (int id = 0; id < rows; id++) {
            dml().writeRow(session(), createNewRow(t,
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
            ordering.append(Expressions.field(tRowType, f), true);
        }
        Operator sort = sort_InsertionLimited(timeSetup, tRowType, ordering, SortOption.PRESERVE_DUPLICATES, rows);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(sort, queryContext);
            cursor.open();
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
