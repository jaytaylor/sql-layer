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

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.akiban.server.test.ExpressionGenerators.*;
import static com.akiban.qp.operator.API.*;

public class Sort_InsertionLimitedIT extends OperatorITBase
{
    @Before
    public void before()
    {
        super.before();
        NewRow[] dbRows = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(customer, 4L, "highland"),
            createNewRow(customer, 5L, "matrix"),
            createNewRow(order, 11L, 1L, "ori"),
            createNewRow(order, 12L, 1L, "david"),
            createNewRow(order, 21L, 2L, "david"),
            createNewRow(order, 22L, 2L, "jack"),
            createNewRow(order, 31L, 3L, "david"),
            createNewRow(order, 51L, 5L, "yuval"),
            createNewRow(item, 111L, 11L),
            createNewRow(item, 112L, 11L),
            createNewRow(item, 121L, 12L),
            createNewRow(item, 122L, 12L),
            createNewRow(item, 211L, 21L),
            createNewRow(item, 212L, 21L),
            createNewRow(item, 221L, 22L),
            createNewRow(item, 222L, 22L),
        };
        use(dbRows);
    }

    // Sort / InsertionLimited tests

    @Test
    public void testCustomerName()
    {
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                customerRowType,
                ordering(field(customerRowType, 1), true),
                SortOption.PRESERVE_DUPLICATES,
                2);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 4L, "highland")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderSalesmanCid()
    {
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                ordering(field(orderRowType, 2), true, field(orderRowType, 1), false),
                SortOption.PRESERVE_DUPLICATES,
                4);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "david"),
            row(orderRowType, 21L, 2L, "david"),
            row(orderRowType, 12L, 1L, "david"),
            row(orderRowType, 22L, 2L, "jack"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderSalesman()
    {
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                ordering(field(orderRowType, 2), true),
                SortOption.PRESERVE_DUPLICATES,
                4);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            // Order among equals in group.
            row(orderRowType, 12L, 1L, "david"),
            row(orderRowType, 21L, 2L, "david"),
            row(orderRowType, 31L, 3L, "david"),
            row(orderRowType, 22L, 2L, "jack"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderSalesman2()
    {
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                ordering(field(orderRowType, 2), true),
                SortOption.PRESERVE_DUPLICATES,
                2);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            // Kept earlier ones in group (fewer inserts).
            row(orderRowType, 12L, 1L, "david"),
            row(orderRowType, 21L, 2L, "david"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAAA()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, true, oidField, true, iidField, true),
                SortOption.PRESERVE_DUPLICATES,
                8);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAAD()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, true, oidField, true, iidField, false),
                SortOption.PRESERVE_DUPLICATES,
                8);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testADA()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, true, oidField, false, iidField, true),
                SortOption.PRESERVE_DUPLICATES,
                8);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testADD()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, true, oidField, false, iidField, false),
                SortOption.PRESERVE_DUPLICATES,
                8);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testDAA()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, false, oidField, true, iidField, true),
                SortOption.PRESERVE_DUPLICATES,
                8);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testDAD()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, false, oidField, true, iidField, false),
                SortOption.PRESERVE_DUPLICATES,
                8);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testDDA()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, false, oidField, false, iidField, true),
                SortOption.PRESERVE_DUPLICATES,
                8);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testDDD()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, false, oidField, false, iidField, false),
                SortOption.PRESERVE_DUPLICATES,
                8);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testPreserveDuplicates()
    {
        Operator project =
            project_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                Arrays.asList(field(orderRowType, 1)));
        RowType projectType = project.rowType();
        Operator plan =
            sort_InsertionLimited(
                project,
                projectType,
                ordering(field(projectType, 0), true),
                SortOption.PRESERVE_DUPLICATES,
                5);

        RowBase[] expected = new RowBase[]{
            row(projectType, 1L),
            row(projectType, 1L),
            row(projectType, 2L),
            row(projectType, 2L),
            row(projectType, 3L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testSuppressDuplicateCID()
    {
        Operator project =
            project_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                Arrays.asList(field(orderRowType, 1)));
        RowType projectType = project.rowType();
        Operator plan =
            sort_InsertionLimited(
                project,
                projectType,
                ordering(field(projectType, 0), true),
                SortOption.SUPPRESS_DUPLICATES,
                4);

        RowBase[] expected = new RowBase[]{
            row(projectType, 1L),
            row(projectType, 2L),
            row(projectType, 3L),
            row(projectType, 5L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testSuppressDuplicateName()
    {
        Operator project =
            project_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                Arrays.asList(field(orderRowType, 2)));
        RowType projectType = project.rowType();
        Operator plan =
            sort_InsertionLimited(
                project,
                projectType,
                ordering(field(projectType, 0), true),
                SortOption.SUPPRESS_DUPLICATES,
                2);

        RowBase[] expected = new RowBase[]{
            row(projectType, "david"),
            row(projectType, "jack"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test 
    public void testFreeze()
    {
        RowType innerValuesRowType = schema.newValuesType(AkType.NULL);
        List<BindableRow> innerValuesRows = new ArrayList<BindableRow>();
        innerValuesRows.add(BindableRow.of(innerValuesRowType, Collections.singletonList(literal(null)), null));
        Operator project = project_Default(valuesScan_Default(innerValuesRows, innerValuesRowType),
                                           innerValuesRowType,
                                           Arrays.asList(boundField(customerRowType, 0, 1)));
        RowType projectType = project.rowType();
        Operator plan =
            sort_InsertionLimited(
                map_NestedLoops(
                    filter_Default(groupScan_Default(coi),
                                   Collections.singleton(customerRowType)),
                    project, 0),
                projectType,
                ordering(field(projectType, 0), true),
                SortOption.PRESERVE_DUPLICATES,
                4);

        RowBase[] expected = new RowBase[]{
            row(projectType, "foundation"),
            row(projectType, "highland"),
            row(projectType, "matrix"),
            row(projectType, "northbridge"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            sort_InsertionLimited(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                ordering(field(orderRowType, 2), true, field(orderRowType, 1), false),
                SortOption.PRESERVE_DUPLICATES,
                4);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(orderRowType, 31L, 3L, "david"),
                    row(orderRowType, 21L, 2L, "david"),
                    row(orderRowType, 12L, 1L, "david"),
                    row(orderRowType, 22L, 2L, "jack"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private Ordering ordering(Object... objects)
    {
        Ordering ordering = API.ordering();
        int i = 0;
        while (i < objects.length) {
            ExpressionGenerator expression = (ExpressionGenerator) objects[i++];
            Boolean ascending = (Boolean) objects[i++];
            ordering.append(expression, ascending);
        }
        return ordering;
    }

}
