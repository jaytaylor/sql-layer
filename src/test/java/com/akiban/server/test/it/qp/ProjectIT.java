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

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.server.test.ExpressionGenerators.field;
import static com.akiban.qp.operator.API.*;
import static com.akiban.qp.operator.API.JoinType.*;

public class ProjectIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
        NewRow[] dbWithOrphans = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(customer, 4L, "highland"),
            createNewRow(order, 11L, 1L, "ori"),
            createNewRow(order, 12L, 1L, "david"),
            createNewRow(order, 21L, 2L, "tom"),
            createNewRow(order, 22L, 2L, "jack"),
            createNewRow(order, 31L, 3L, "peter"),
            createNewRow(item, 111L, 11L),
            createNewRow(item, 112L, 11L),
            createNewRow(item, 121L, 12L),
            createNewRow(item, 122L, 12L),
            createNewRow(item, 211L, 21L),
            createNewRow(item, 212L, 21L),
            createNewRow(item, 221L, 22L),
            createNewRow(item, 222L, 22L),
            // orphans
            createNewRow(item, 311L, 31L),
            createNewRow(item, 312L, 31L)};
        use(dbWithOrphans);
    }

    // IllegalArumentException tests

    @Test(expected = IllegalArgumentException.class)
    public void testNullRowType()
    {
        project_Default(groupScan_Default(coi), null, Arrays.asList(field(customerRowType, 0)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullProjections()
    {
        project_Default(groupScan_Default(coi), customerRowType, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyProjections()
    {
        project_Default(groupScan_Default(coi), customerRowType, Collections.<ExpressionGenerator>emptyList());
    }

    // Projection tests

    @Test
    public void testCustomerCid()
    {
        Operator plan = project_Default(groupScan_Default(coi),
                                                customerRowType,
                                                Arrays.asList(field(customerRowType, 0)));
        Cursor cursor = cursor(plan, queryContext);
        RowType projectedRowType = plan.rowType();
        RowBase[] expected = new RowBase[]{
            row(projectedRowType, 1L),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(projectedRowType, 2L),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(projectedRowType, 4L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testReverseCustomerColumns()
    {
        Operator plan = project_Default(groupScan_Default(coi),
                                                customerRowType,
                                                Arrays.asList(field(customerRowType, 1), field(customerRowType, 0)));
        Cursor cursor = cursor(plan, queryContext);
        RowType projectedRowType = plan.rowType();
        RowBase[] expected = new RowBase[]{
            row(projectedRowType, "northbridge", 1L),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(projectedRowType, "foundation", 2L),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(projectedRowType, "highland", 4L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testProjectOfFlatten()
    {
        // Tests projection of null too
        Operator flattenCO = flatten_HKeyOrdered(groupScan_Default(coi),
                                                         customerRowType,
                                                         orderRowType,
                                                         FULL_JOIN);
        RowType coType = flattenCO.rowType();
        Operator flattenCOI = flatten_HKeyOrdered(flattenCO,
                                                          coType,
                                                          itemRowType,
                                                          FULL_JOIN);
        RowType coiType = flattenCOI.rowType();
        Operator plan =
            project_Default(flattenCOI,
                            coiType,
                            Arrays.asList(
                                field(coiType, 1), // customer name
                                field(coiType, 4), // salesman
                                field(coiType, 5))); // iid
        Cursor cursor = cursor(plan, queryContext);
        RowType projectedRowType = plan.rowType();
        RowBase[] expected = new RowBase[]{
            row(projectedRowType, "northbridge", "ori", 111L),
            row(projectedRowType, "northbridge", "ori", 112L),
            row(projectedRowType, "northbridge", "david", 121L),
            row(projectedRowType, "northbridge", "david", 122L),
            row(projectedRowType, "foundation", "tom", 211L),
            row(projectedRowType, "foundation", "tom", 212L),
            row(projectedRowType, "foundation", "jack", 221L),
            row(projectedRowType, "foundation", "jack", 222L),
            row(projectedRowType, null, "peter", 311L),
            row(projectedRowType, null, "peter", 312L),
            row(projectedRowType, "highland", null, null)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        // Tests projection of null too
        Operator flattenCO = flatten_HKeyOrdered(groupScan_Default(coi),
                                                         customerRowType,
                                                         orderRowType,
                                                         FULL_JOIN);
        RowType coType = flattenCO.rowType();
        Operator flattenCOI = flatten_HKeyOrdered(flattenCO,
                                                          coType,
                                                          itemRowType,
                                                          FULL_JOIN);
        RowType coiType = flattenCOI.rowType();
        Operator plan =
            project_Default(flattenCOI,
                            coiType,
                            Arrays.asList(
                                field(coiType, 1), // customer name
                                field(coiType, 4), // salesman
                                field(coiType, 5))); // iid
        final RowType projectedRowType = plan.rowType();
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(projectedRowType, "northbridge", "ori", 111L),
                    row(projectedRowType, "northbridge", "ori", 112L),
                    row(projectedRowType, "northbridge", "david", 121L),
                    row(projectedRowType, "northbridge", "david", 122L),
                    row(projectedRowType, "foundation", "tom", 211L),
                    row(projectedRowType, "foundation", "tom", 212L),
                    row(projectedRowType, "foundation", "jack", 221L),
                    row(projectedRowType, "foundation", "jack", 222L),
                    row(projectedRowType, null, "peter", 311L),
                    row(projectedRowType, null, "peter", 312L),
                    row(projectedRowType, "highland", null, null)
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }
}
