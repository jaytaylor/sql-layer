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

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.Expressions;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.operator.API.*;
import static org.junit.Assert.fail;

// Like Select_BloomFilterIT, but testing with case-insensitive strings

public class Select_BloomFilter_CaseInsensitive_IT extends OperatorITBase
{
    @Before
    public void before()
    {
        // Tables are Driving (D) and Filtering (F). Find Filtering rows with a given test id, yielding
        // a set of (a, b) rows. Then find Driving rows matching a and b.
        d = createTable(
            "schema", "driving",
            "test_id int not null",
            "a varchar(10) collate latin1_swedish_ci",
            "b varchar(10) collate latin1_swedish_ci");
        f = createTable(
            "schema", "filtering",
            "test_id int not null",
            "a varchar(10) collate latin1_swedish_ci",
            "b varchar(10) collate latin1_swedish_ci");
        Index dIndex = createIndex("schema", "driving", "idx_d", "test_id", "a", "b");
        Index fab = createIndex("schema", "filtering", "idx_fab", "a", "b");
        schema = new Schema(rowDefCache().ais());
        dRowType = schema.userTableRowType(userTable(d));
        fRowType = schema.userTableRowType(userTable(f));
        dIndexRowType = dRowType.indexRowType(dIndex);
        fabIndexRowType = fRowType.indexRowType(fab);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[]{
/*
            // Test 0: No d or f rows
            // Test 1: No f rows
            createNewRow(f, 1L, 10L, 100L),
            // Test 2: No d rows
            createNewRow(f, 2L, 20L, 200L),
            // Test 3: 1 d row, no matching f rows
            createNewRow(d, 3L, 30L, 300L),
            createNewRow(f, 3L, 31L, 300L),
            createNewRow(f, 3L, 30L, 301L),
            // Test 4: 1 d row, 1 matching f rows
            createNewRow(d, 4L, 40L, 400L),
            createNewRow(f, 4L, 40L, 400L),
            createNewRow(f, 4L, 41L, 400L),
            createNewRow(f, 4L, 40L, 401L),
            // Test 5: multiple d rows, no matching f rows
            createNewRow(d, 5L, 50L, 500L),
            createNewRow(d, 5L, 51L, 501L),
            createNewRow(f, 5L, 50L, 501L),
            createNewRow(f, 5L, 51L, 500L),
*/
            // Test 6: multiple d rows, multiple f rows, multiple matches
            createNewRow(d, 6L, "xy", "A"),
            createNewRow(d, 6L, "XY", "aB"),
            createNewRow(d, 6L, "Xy", "Ac"),
            createNewRow(d, 6L, "xY", "a"),
            createNewRow(f, 6L, "XY", "aZ"),
            createNewRow(f, 6L, "xy",  "Ab"),
            createNewRow(f, 6L, "Xy", "ac"),
            createNewRow(f, 6L, "xy", "aZ"),
            // Test 7: Null columns in d
            createNewRow(d, 7L, null, null),
            // Test 8: Null columns in f
            createNewRow(f, 8L, null, null),
        };
        use(db);
    }

    // Test argument validation

/*
    @Test
    public void testBadInputs()
    {
        try {
            using_BloomFilter(null, customerRowType, 10, 0, groupScan_Default(coi));
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            using_BloomFilter(groupScan_Default(coi), null, 10, 0, groupScan_Default(coi));
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            using_BloomFilter(groupScan_Default(coi), customerRowType, -1, 0, groupScan_Default(coi));
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            using_BloomFilter(groupScan_Default(coi), customerRowType, 10, -1, groupScan_Default(coi));
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            using_BloomFilter(groupScan_Default(coi), customerRowType, 10, -1, null);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    // Test operator execution

    @Test
    public void test0()
    {
        Operator plan = plan(0);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test1()
    {
        Operator plan = plan(1);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test2()
    {
        Operator plan = plan(2);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test3()
    {
        Operator plan = plan(3);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test4()
    {
        Operator plan = plan(4);
        RowBase[] expected = new RowBase[] {
            row(outputRowType, 4L, 40L, 400L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test5()
    {
        Operator plan = plan(5);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext));
    }
*/

    @Test
    public void test6()
    {
        Operator plan = plan(6);
        RowBase[] expected = new RowBase[] {
            row(outputRowType, 6L, 61L, 601L),
            row(outputRowType, 6L, 62L, 602L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test7()
    {
        Operator plan = plan(7);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test8()
    {
        Operator plan = plan(8);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCursor()
    {
        Operator plan = plan(6);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(outputRowType, 6L, 61L, 601L),
                    row(outputRowType, 6L, 62L, 602L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    public Operator plan(long testId)
    {
        // loadFilter loads the filter with F rows containing the given testId.
        Operator loadFilter = project_Default(
            select_HKeyOrdered(
                filter_Default(
                    groupScan_Default(groupTable(f)),
                    Collections.singleton(fRowType)),
                fRowType,
                Expressions.compare(
                    Expressions.field(fRowType, 0),
                    Comparison.EQ,
                    Expressions.literal(testId))),
            fRowType,
            Arrays.asList(Expressions.field(fRowType, 1),
                          Expressions.field(fRowType, 2)));
        // For the index scan retriving rows from the D(test_id) index
        IndexBound testIdBound =
            new IndexBound(row(dIndexRowType, testId), new SetColumnSelector(0));
        IndexKeyRange dTestIdKeyRange =
            IndexKeyRange.bounded(dIndexRowType, testIdBound, true, testIdBound, true);
        // For the  index scan retrieving rows from the F(a, b) index given a D index row
        IndexBound abBound = new IndexBound(
            new RowBasedUnboundExpressions(
                loadFilter.rowType(),
                Arrays.asList(
                    Expressions.boundField(dIndexRowType, 0, 1),
                    Expressions.boundField(dIndexRowType, 0, 2))),
            new SetColumnSelector(0, 1));
        IndexKeyRange fabKeyRange =
            IndexKeyRange.bounded(fabIndexRowType, abBound, true, abBound, true);
        // Use a bloom filter loaded by loadFilter. Then for each input row, check the filter (projecting
        // D rows on (a, b)), and, for positives, check F using an index scan keyed by D.a and D.b.
        Operator plan =
            project_Default(
                using_BloomFilter(
                    // filterInput
                    loadFilter,
                    // filterRowType
                    loadFilter.rowType(),
                    // estimatedRowCount
                    10,
                    // filterBindingPosition
                    0,
                    // streamInput
                    select_BloomFilter(
                        // input
                        indexScan_Default(dIndexRowType, dTestIdKeyRange, new Ordering()),
                        // onPositive
                        indexScan_Default(
                            fabIndexRowType,
                            fabKeyRange,
                            new Ordering()),
                        // filterFields
                        Arrays.asList(
                            Expressions.field(dIndexRowType, 1),
                            Expressions.field(dIndexRowType, 2)),
                        // filterBindingPosition
                        0)),
                dIndexRowType,
                Arrays.asList(
                    Expressions.field(dIndexRowType, 0),   // test_id
                    Expressions.field(dIndexRowType, 1),   // a
                    Expressions.field(dIndexRowType, 2))); // b
        outputRowType = plan.rowType();
        return plan;
    }

    private int d;
    private int f;
    private UserTableRowType dRowType;
    private UserTableRowType fRowType;
    private RowType outputRowType;
    IndexRowType dIndexRowType;
    IndexRowType fabIndexRowType;
}
