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

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.field;
import static junit.framework.Assert.fail;

public class Union_OrderedIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "pid int not null primary key",
            "x int");
        createIndex("schema", "t", "idx_x", "x");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        tPidIndexRowType = indexType(t, "pid");
        tXIndexRowType = indexType(t, "x");
        coi = group(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[] {
            createNewRow(t, 1000L, 1L),
            createNewRow(t, 1001L, 1L),
            createNewRow(t, 1002L, 1L),
            createNewRow(t, 2000L, 2L),
            createNewRow(t, 2001L, 2L),
            createNewRow(t, 2002L, 2L),
        };
        use(db);
    }

    private int t;
    private RowType tRowType;
    private IndexRowType tPidIndexRowType;
    private IndexRowType tXIndexRowType;

    // IllegalArumentException tests

    @Test
    public void testInputNull()
    {
        // First input null
        try {
            union_Ordered(null,
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          1,
                          ascending(true));
        } catch (IllegalArgumentException e) {
        }
        // Second input null
        try {
            union_Ordered(groupScan_Default(coi),
                          null,
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          1,
                          ascending(true));
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testInputType()
    {
        // First input type null
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          null,
                          tXIndexRowType,
                          1,
                          1,
                          ascending(true));
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Second input type null
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          null,
                          1,
                          1,
                          ascending(true));
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Test different input types
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tPidIndexRowType,
                          1,
                          1,
                          ascending(true));
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testOrderingColumns()
    {
        // First ordering fields negative
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          -1,
                          1,
                          ascending(true));
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Second ordering fields negative
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          -1,
                          ascending(true));
            fail();
        } catch (IllegalArgumentException e) {
        }
        // First ordering fields too high
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          3,
                          1,
                          ascending(true));
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Second ordering fields too high
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          3,
                          ascending(true));
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Different number of ordering fields
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          2,
                          ascending(true));
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    // Runtime tests

    @Test
    public void testBothInputsEmpty()
    {
        Operator plan = unionPlan(0, 0, true);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = unionPlan(0, 0, false);
        expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testLeftEmpty()
    {
        Operator plan = unionPlan(0, 1, true);
        RowBase[] expected = new RowBase[] {
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = unionPlan(0, 1, false);
        expected = new RowBase[] {
            row(tRowType, 1L, 1002L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testRightEmpty()
    {
        Operator plan = unionPlan(1, 0, true);
        RowBase[] expected = new RowBase[] {
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = unionPlan(1, 0, false);
        expected = new RowBase[] {
            row(tRowType, 1L, 1002L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testDuplicates()
    {
        Operator plan = unionPlan(1, 1, true);
        RowBase[] expected = new RowBase[] {
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = unionPlan(1, 1, false);
        expected = new RowBase[] {
            row(tRowType, 1L, 1002L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testDisjoint()
    {
        Operator plan = unionPlan(1, 2, true);
        RowBase[] expected = new RowBase[] {
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1002L),
            row(tRowType, 2L, 2000L),
            row(tRowType, 2L, 2001L),
            row(tRowType, 2L, 2002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = unionPlan(1, 2, false);
        expected = new RowBase[] {
            row(tRowType, 2L, 2002L),
            row(tRowType, 2L, 2001L),
            row(tRowType, 2L, 2000L),
            row(tRowType, 1L, 1002L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    private Operator unionPlan(int k1, int k2, boolean ascending)
    {
        Operator plan =
            union_Ordered(
                indexScan_Default(
                    tXIndexRowType,
                    parentXEq(k1),
                    ordering(field(tXIndexRowType, 1), ascending)),
                indexScan_Default(
                    tXIndexRowType,
                    parentXEq(k2),
                    ordering(field(tXIndexRowType, 1), ascending)),
                tXIndexRowType,
                tXIndexRowType,
                1,
                1,
                ascending(ascending));
        return plan;
    }

    private IndexKeyRange parentXEq(long x)
    {
        IndexBound xBound = new IndexBound(row(tXIndexRowType, x), new SetColumnSelector(0));
        return IndexKeyRange.bounded(tXIndexRowType, xBound, true, xBound, true);
    }

    private Ordering ordering(Object... objects)
    {
        Ordering ordering = API.ordering();
        int i = 0;
        while (i < objects.length) {
            Expression expression = (Expression) objects[i++];
            Boolean ascending = (Boolean) objects[i++];
            ordering.append(expression, ascending);
        }
        return ordering;
    }

    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
}
