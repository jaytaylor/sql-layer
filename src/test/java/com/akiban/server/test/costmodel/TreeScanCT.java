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

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;

public class TreeScanCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        run(CostModelColumn.intColumn("x"));
        run(CostModelColumn.varcharColumnGoodPrefixCompression("x", 10));
        run(CostModelColumn.varcharColumnGoodPrefixCompression("x", 100));
        run(CostModelColumn.varcharColumnGoodPrefixCompression("x", 1000));
        run(CostModelColumn.varcharColumnBadPrefixCompression("x", 10));
        run(CostModelColumn.varcharColumnBadPrefixCompression("x", 100));
        run(CostModelColumn.varcharColumnBadPrefixCompression("x", 1000));
    }

    private void run(CostModelColumn indexedColumn)
    {
        this.indexedColumn = indexedColumn;
        createSchema();
        populateDB(ROWS);
        runSequential(WARMUP_RUNS, 1, null);
        String label = indexedColumn.description();
        runSequential(MEASURED_RUNS, 1000, label);
        runRandom(MEASURED_RUNS, 1000, label);
        runSequential(MEASURED_RUNS, 500, label);
        runRandom(MEASURED_RUNS, 500, label);
        runSequential(MEASURED_RUNS, 250, label);
        runRandom(MEASURED_RUNS, 250, label);
        runSequential(MEASURED_RUNS, 100, label);
        runRandom(MEASURED_RUNS, 100, label);
        runSequential(MEASURED_RUNS, 50, label);
        runRandom(MEASURED_RUNS, 50, label);
        runSequential(MEASURED_RUNS, 25, label);
        runRandom(MEASURED_RUNS, 25, label);
        runSequential(MEASURED_RUNS, 10, label);
        runRandom(MEASURED_RUNS, 10, label);
        runSequential(MEASURED_RUNS, 9, label);
        runRandom(MEASURED_RUNS, 9, label);
        runSequential(MEASURED_RUNS, 8, label);
        runRandom(MEASURED_RUNS, 8, label);
        runSequential(MEASURED_RUNS, 7, label);
        runRandom(MEASURED_RUNS, 7, label);
        runSequential(MEASURED_RUNS, 6, label);
        runRandom(MEASURED_RUNS, 6, label);
        runSequential(MEASURED_RUNS, 5, label);
        runRandom(MEASURED_RUNS, 5, label);
        runSequential(MEASURED_RUNS, 4, label);
        runRandom(MEASURED_RUNS, 4, label);
        runSequential(MEASURED_RUNS, 3, label);
        runRandom(MEASURED_RUNS, 3, label);
        runSequential(MEASURED_RUNS, 2, label);
        runRandom(MEASURED_RUNS, 2, label);
        runSequential(MEASURED_RUNS, 1, label);
        runRandom(MEASURED_RUNS, 1, label);
        runSequential(MEASURED_RUNS, 100, label);
        runRandom(MEASURED_RUNS, 100, label);
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
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void populateDB(int rows)
    {
        start = rows / 2;
        keys = new Object[MAX_SCAN];
        int k = 0;
        for (int id = 0; id < rows; id++) {
            Object key = indexedColumn.valueFor(id);
            dml().writeRow(session(), createNewRow(t, id, key));
            if (id >= start && k < keys.length) {
                keys[k++] = key;
            }
        }
    }

    private void runSequential(int runs, int sequentialAccessesPerRandom, String label)
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
            System.out.println(String.format("SEQUENTIAL %s - %s:  %s usec/row", label, sequentialAccessesPerRandom, averageUsec));
        }
    }

    // Do random accesses instead of sequential. Nearby random accesses are potentially faster.
    private void runRandom(int runs, int sequentialAccessesPerRandom, String label)
    {
        ValuesHolderRow boundRow = new ValuesHolderRow(idxRowType);
        ValueHolder valueHolder = boundRow.holderAt(0);
        queryContext.setRow(0, boundRow);
        IndexBound bound = new IndexBound(boundRow, new SetColumnSelector(0));
        IndexKeyRange keyRange = IndexKeyRange.bounded(idxRowType, bound, true, bound, true);
        Ordering ordering = new Ordering();
        ordering.append(new FieldExpression(idxRowType, 0), true);
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        Cursor cursor = cursor(plan, queryContext);
        long startTime = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            for (int s = 0; s < sequentialAccessesPerRandom; s++) {
                Object key = keys[s];
                if (key instanceof Integer) {
                    valueHolder.putInt((long) ((Integer) key).intValue());
                } else {
                    valueHolder.putString((String) key);
                }
                cursor.open();
                Row row = cursor.next();
                assert row != null;
                cursor.close();
            }
        }
        long endTime = System.nanoTime();
        if (label != null) {
            double averageUsec = (endTime - startTime) / (1000.0 * runs * sequentialAccessesPerRandom);
            System.out.println(String.format("RANDOM %s - %s:  %s usec/row", label, sequentialAccessesPerRandom, averageUsec));
        }
    }

    private static final int ROWS = 2000000; // Should create 3-level trees
    private static final int WARMUP_RUNS = 20000;
    private static final int MEASURED_RUNS = 10000;
    private static final int MAX_SCAN = 1000;

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private int start;
    private Object[] keys;
    private CostModelColumn indexedColumn;
}
