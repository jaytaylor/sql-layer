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
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.ExpressionGenerators;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.operator.API.*;

public class Select_BloomFilterCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        // Load-only
        run(WARMUP_RUNS, true, 0, false);
        run(MEASURED_RUNS, true, 0, true);
        // Load and scan. Vary the starting value to control the amount of overlap with the filter rows.
        run(WARMUP_RUNS, false, 0, false);
        for (int start = 0; start <= FILTER_ROWS; start += FILTER_ROWS / 10) {
            run(MEASURED_RUNS, false, start, true);
        }
    }

    private void createSchema() throws InvalidOperationException
    {
        // Schema is similar to that in Select_BloomFilterIT
        String schemaName = schemaName();
        String dTableName = newTableName(); // Driving table
        String fTableName = newTableName(); // Filtering table
        d = createTable(
            schemaName, dTableName,
            "x int");
        f = createTable(
            schemaName, fTableName,
            "x int");
        Index dx = createIndex(schemaName, dTableName, "idx_dx", "x");
        Index fx = createIndex(schemaName, fTableName, "idx_fx", "x");
        schema = new Schema(rowDefCache().ais());
        dRowType = schema.userTableRowType(userTable(d));
        fRowType = schema.userTableRowType(userTable(f));
        dIndexRowType = dRowType.indexRowType(dx);
        fIndexRowType = fRowType.indexRowType(fx);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        schema = new Schema(rowDefCache().ais());
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void populateDB()
    {
        for (int x = 0; x < FILTER_ROWS; x++) {
            dml().writeRow(session(), createNewRow(f, x, x)); // x, akiban_pk
        }
        for (int x = 0; x < DRIVING_ROWS; x++) {
            dml().writeRow(session(), createNewRow(d, x, x)); // x, akiban_pk
        }
    }
    
    private void run(int runs, boolean loadOnly, int startScan, boolean report)
    {
        Operator plan = loadOnly ? planLoadOnly() : planLoadAndSelect(startScan);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            Row row;
            while ((row = cursor.next()) != null) {
                // System.out.println(row);
            }
        }
        long stop = System.nanoTime();
        long planNsec = stop - start;
        if (loadOnly) {
            planNsec -= timeFilterInput.elapsedNsec();
            if (report) {
                double averageUsecPerRow = planNsec / (1000.0 * runs * FILTER_ROWS);
                System.out.println(String.format("load only: %s usec/row", averageUsecPerRow));
            }
        } else {
            planNsec -= (timeFilterInput.elapsedNsec() + timeScanInput.elapsedNsec());
            if (report) {
                double averageUsecPerRow = planNsec / (1000.0 * runs * (DRIVING_ROWS - startScan));
                double selected = (double) (FILTER_ROWS - startScan) / DRIVING_ROWS;
                System.out.println(String.format("scan %s: %s usec/row", selected, averageUsecPerRow));
            }
        }
    }

    public Operator planLoadOnly()
    {
        // filterInput loads the filter with F rows containing the given testId.
        Operator filterInput =
            project_Default(
                groupScan_Default(group(f)),
                fRowType,
                Arrays.asList(ExpressionGenerators.field(fRowType, 0)));
        timeFilterInput = new TimeOperator(filterInput);
        // For the  index scan retrieving rows from the F(x) index given a D index row
        IndexBound fxBound = new IndexBound(
            new RowBasedUnboundExpressions(
                filterInput.rowType(),
                Arrays.asList(ExpressionGenerators.boundField(dIndexRowType, 0, 0))),
            new SetColumnSelector(0));
        IndexKeyRange fKeyRange = IndexKeyRange.bounded(fIndexRowType, fxBound, true, fxBound, true);
        // Use a bloom filter loaded by filterInput. Then for each input row, check the filter (projecting
        // D rows on (x)), and, for positives, check F using an index scan keyed by D.x.
        Operator plan =
            using_BloomFilter(
                // filterInput
                timeFilterInput,
                // filterRowType
                filterInput.rowType(),
                // estimatedRowCount
                FILTER_ROWS,
                // filterBindingPosition
                0,
                // streamInput
                select_BloomFilter(
                    // input
                    valuesScan_Default(Collections.<BindableRow>emptyList(), dIndexRowType),
                    // onPositive
                    indexScan_Default(
                        fIndexRowType,
                        fKeyRange,
                        new Ordering()),
                    // filterFields
                    Arrays.asList(ExpressionGenerators.field(dIndexRowType, 0)),
                    // filterBindingPosition
                    0));
        return plan;
    }

    public Operator planLoadAndSelect(int start)
    {
        // filterInput loads the filter with F rows containing the given testId.
        Operator filterInput =
            project_Default(
                groupScan_Default(group(f)),
                fRowType,
                Arrays.asList(ExpressionGenerators.field(fRowType, 0)));
        timeFilterInput = new TimeOperator(filterInput);
        // For the index scan retriving rows from the D(x) index
        IndexBound dxLo =
            new IndexBound(row(dIndexRowType, start), new SetColumnSelector(0));
        IndexBound dxHi =
            new IndexBound(row(dIndexRowType, Integer.MAX_VALUE), new SetColumnSelector(0));
        IndexKeyRange dKeyRange =
            IndexKeyRange.bounded(dIndexRowType, dxLo, true, dxHi, false);
        // For the  index scan retrieving rows from the F(x) index given a D index row
        IndexBound fxBound = new IndexBound(
            new RowBasedUnboundExpressions(
                filterInput.rowType(),
                Arrays.asList(ExpressionGenerators.boundField(dIndexRowType, 0, 0))),
            new SetColumnSelector(0));
        IndexKeyRange fKeyRange = IndexKeyRange.bounded(fIndexRowType, fxBound, true, fxBound, true);
        // Use a bloom filter loaded by filterInput. Then for each input row, check the filter (projecting
        // D rows on (x)), and, for positives, check F using an index scan keyed by D.x.
        Operator scanInput = indexScan_Default(dIndexRowType, dKeyRange, new Ordering());
        timeScanInput = new TimeOperator(scanInput);
        Operator plan =
            using_BloomFilter(
                // filterInput
                timeFilterInput,
                // filterRowType
                filterInput.rowType(),
                // estimatedRowCount
                FILTER_ROWS,
                // filterBindingPosition
                0,
                // streamInput
                select_BloomFilter(
                    // input
                    timeScanInput,
                    // onPositive
                    indexScan_Default(
                        fIndexRowType,
                        fKeyRange,
                        new Ordering()),
                    // filterFields
                    Arrays.asList(ExpressionGenerators.field(dIndexRowType, 0)),
                    // filterBindingPosition
                    0));
        return plan;
    }

    private static final int WARMUP_RUNS = 2;
    private static final int MEASURED_RUNS = 5;
    private static final long FILTER_ROWS = 100000;
    private static final long DRIVING_ROWS = 200000;

    private int d;
    private int f;
    private UserTableRowType dRowType;
    private UserTableRowType fRowType;
    private IndexRowType dIndexRowType;
    private IndexRowType fIndexRowType;
    private TimeOperator timeFilterInput;
    private TimeOperator timeScanInput;
}
