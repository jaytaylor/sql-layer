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
import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.expression.std.Expressions;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.operator.API.*;
import static com.akiban.qp.operator.API.indexScan_Default;

public class Select_BloomFilterCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        run(WARMUP_RUNS, false);
        run(MEASURED_RUNS, true);
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
        Index dIndex = createIndex(schemaName, dTableName, "idx_dx", "x");
        Index fab = createIndex(schemaName, fTableName, "idx_fx", "x");
        schema = new Schema(rowDefCache().ais());
        dRowType = schema.userTableRowType(userTable(d));
        fRowType = schema.userTableRowType(userTable(f));
        fIndexRowType = fRowType.indexRowType(fab);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        schema = new Schema(rowDefCache().ais());
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void populateDB()
    {
        for (int f = 0; f < FILTER_ROWS; f++) {
            dml().writeRow(session(), createNewRow(f, f)); // x, akiban_pk
        }
        for (int d = 0; d < DRIVING_ROWS; d++) {
            dml().writeRow(session(), createNewRow(d, d)); // x, akiban_pk
        }
    }
    
    private void run(int runs, boolean report)
    {
/*
        Operator setupOuter = groupScan_Default(group);
        TimeOperator timeSetupOuter = new TimeOperator(setupOuter);
        Operator setupInner = limit_Default(groupScan_Default(group), innerRows);
        TimeOperator timeSetupInner = new TimeOperator(setupInner);
        Operator plan = map_NestedLoops(timeSetupOuter, timeSetupInner, 0);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long mapNsec = stop - start - timeSetupInner.elapsedNsec() - timeSetupOuter.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = mapNsec / (1000.0 * runs * (FILTER_ROWS * (innerRows + 1)));
            System.out.println(String.format("inner/outer = %s: %s usec/row",
                                             innerRows, averageUsecPerRow));
        }
*/
    }

    public Operator planLoadOnly()
    {
        // loadFilter loads the filter with F rows containing the given testId.
        Operator filterInput =
            project_Default(
                groupScan_Default(groupTable(f)),
                fRowType,
                Arrays.asList(Expressions.field(fRowType, 0)));
        timeFilterInput = new TimeOperator(filterInput);
        // For the  index scan retrieving rows from the F(a, b) index given a D index row
        IndexBound xBound = new IndexBound(
            new RowBasedUnboundExpressions(
                filterInput.rowType(),
                Arrays.asList(Expressions.boundField(dRowType, 0, 0))),
            new SetColumnSelector(0));
        IndexKeyRange fKeyRange = IndexKeyRange.bounded(fIndexRowType, xBound, true, xBound, true);
        // Use a bloom filter loaded by loadFilter. Then for each input row, check the filter (projecting
        // D rows on (x)), and, for positives, check F using an index scan keyed by D.x.
        Operator plan =
            using_BloomFilter(
                // filterInput
                filterInput,
                // filterRowType
                filterInput.rowType(),
                // estimatedRowCount
                FILTER_ROWS,
                // filterBindingPosition
                0,
                // streamInput
                select_BloomFilter(
                    // input
                    valuesScan_Default(Collections.<BindableRow>emptyList(), dRowType),
                    // onPositive
                    indexScan_Default(
                        fIndexRowType,
                        fKeyRange,
                        new Ordering()),
                    // filterFields
                    Arrays.asList(Expressions.field(dRowType, 0)),
                    // filterBindingPosition
                    0));
        return plan;
    }

    public Operator plan()
    {
        // loadFilter loads the filter with F rows containing the given testId.
        Operator loadFilter =
            project_Default(
                groupScan_Default(groupTable(f)),
                fRowType,
                Arrays.asList(Expressions.field(fRowType, 0)));
        TimeOperator timeLoadFilter = new TimeOperator(loadFilter);
        // For the  index scan retrieving rows from the F(a, b) index given a D index row
        IndexBound xBound = new IndexBound(
            new RowBasedUnboundExpressions(
                loadFilter.rowType(),
                Arrays.asList(Expressions.boundField(dRowType, 0, 0))),
            new SetColumnSelector(0));
        IndexKeyRange fKeyRange = IndexKeyRange.bounded(fIndexRowType, xBound, true, xBound, true);
        // Use a bloom filter loaded by loadFilter. Then for each input row, check the filter (projecting
        // D rows on (x)), and, for positives, check F using an index scan keyed by D.x.
        Operator plan =
            using_BloomFilter(
                // filterInput
                loadFilter,
                // filterRowType
                loadFilter.rowType(),
                // estimatedRowCount
                FILTER_ROWS,
                // filterBindingPosition
                0,
                // streamInput
                select_BloomFilter(
                    // input
                    groupScan_Default(groupTable(d)),
                    // onPositive
                    indexScan_Default(
                        fIndexRowType,
                        fKeyRange,
                        new Ordering()),
                    // filterFields
                    Arrays.asList(Expressions.field(dRowType, 0)),
                    // filterBindingPosition
                    0));
        return plan;
    }

    private static final int WARMUP_RUNS = 2;
    private static final int MEASURED_RUNS = 5;
    private static final int FILTER_ROWS = 1000;
    private static final int DRIVING_ROWS = 10000;

    private int d;
    private int f;
    private UserTableRowType dRowType;
    private UserTableRowType fRowType;
    private IndexRowType fIndexRowType;
    private GroupTable group;
    private TimeOperator timeFilterInput;
}
