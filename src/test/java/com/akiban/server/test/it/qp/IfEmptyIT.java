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


import com.akiban.qp.expression.ExpressionRow;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.ProjectedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.BoundFieldExpression;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.types.AkType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.akiban.qp.operator.API.ancestorLookup_Default;
import static com.akiban.qp.operator.API.ancestorLookup_Nested;
import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.filter_Default;
import static com.akiban.qp.operator.API.groupScan_Default;
import static com.akiban.qp.operator.API.ifEmpty_Default;
import static com.akiban.qp.operator.API.indexScan_Default;
import static com.akiban.qp.operator.API.map_NestedLoops;
import static com.akiban.qp.operator.API.ordering;
import static com.akiban.qp.operator.API.project_Default;
import static com.akiban.qp.operator.API.select_HKeyOrdered;
import static com.akiban.qp.operator.API.valuesScan_Default;
import static com.akiban.server.expression.std.Expressions.boundField;
import static com.akiban.server.expression.std.Expressions.compare;
import static com.akiban.server.expression.std.Expressions.field;
import static com.akiban.server.expression.std.Expressions.literal;

public class IfEmptyIT extends OperatorITBase
{
    @Before
    public void before()
    {
        super.before();
        NewRow[] db = new NewRow[]{
            createNewRow(customer, 0L, "matrix"), // no orders
            createNewRow(customer, 2L, "foundation"), // two orders
            createNewRow(order, 200L, 2L, "david"),
            createNewRow(order, 201L, 2L, "david"),
        };
        use(db);
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testInputNull()
    {
        ifEmpty_Default(null, customerRowType, Collections.<Expression>emptyList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRowTypeNull()
    {
        ifEmpty_Default(groupScan_Default(coi), null, Collections.<Expression>emptyList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExprsNull()
    {
        ifEmpty_Default(groupScan_Default(coi), customerRowType, null);
    }

    // Test operator execution

    @Test
    public void testNonEmpty()
    {
        Operator plan =
            ifEmpty_Default(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(2), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                    API.LookupOption.DISCARD_INPUT),
                orderRowType,
                Arrays.asList(literal(999), literal(999), literal("herman")));
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 200L, 2L, "david"),
            row(orderRowType, 201L, 2L, "david"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testEmpty()
    {
        Operator plan =
            ifEmpty_Default(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(0), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                    API.LookupOption.DISCARD_INPUT),
                orderRowType,
                Arrays.asList(literal(999), literal(999), literal("herman")));
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 999L, 999L, "herman"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCursorNonEmpty()
    {
        Operator plan =
            ifEmpty_Default(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(2), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                    API.LookupOption.DISCARD_INPUT),
                orderRowType,
                Arrays.asList(literal(999), literal(999), literal("herman")));
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(orderRowType, 200L, 2L, "david"),
                    row(orderRowType, 201L, 2L, "david"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test
    public void testCursorEmpty()
    {
        Operator plan =
            ifEmpty_Default(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(0), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                    API.LookupOption.DISCARD_INPUT),
                orderRowType,
                Arrays.asList(literal(999), literal(999), literal("herman")));
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(orderRowType, 999L, 999L, "herman"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private IndexKeyRange cidKeyRange(int cid)
    {
        IndexBound bound = new IndexBound(row(orderCidIndexRowType, cid), new SetColumnSelector(0));
        return IndexKeyRange.bounded(orderCidIndexRowType, bound, true, bound, true);
    }

    private API.Ordering asc()
    {
        API.Ordering ordering = new API.Ordering();
        ordering.append(new FieldExpression(orderCidIndexRowType, 0), true);
        return ordering;
    }
}
