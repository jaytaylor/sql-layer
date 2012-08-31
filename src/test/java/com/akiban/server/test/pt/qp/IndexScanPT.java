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

package com.akiban.server.test.pt.qp;

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
            "id int not null",
            "x int",
            "primary key(id)");
        createIndex("schema", "t", "idx_x", "x");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "x");
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    @Test
    public void profileIndexScan()
    {
        Tap.setEnabled(".*", false);
        populateDB(ROWS);
        run(null, WARMUP_RUNS, 1);
        run("0", MEASURED_RUNS, 1);
/*
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
*/
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
            cursor.destroy();
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
    private static final int MEASURED_RUNS = 1000000000;

    private final Random random = new Random();
    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
}
