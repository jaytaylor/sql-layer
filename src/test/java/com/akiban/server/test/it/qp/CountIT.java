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
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static com.akiban.qp.operator.API.*;

public class CountIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        NewRow[] dbRows = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(customer, 4L, "highland"),
            createNewRow(order, 11L, 1L, "ori")
        };
        use(dbRows);
    }

    // Count tests

    @Test
    public void testCustomerCid()
    {
        Operator plan = count_Default(groupScan_Default(coi),
                                              customerRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowType resultRowType = plan.rowType();
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 11L, 1L, "ori"),
            row(resultRowType, 3L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCustomers()
    {
        Operator plan = count_TableStatus(customerRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowType resultRowType = plan.rowType();
        RowBase[] expected = new RowBase[]{
            row(resultRowType, 3L)
        };
        compareRows(expected, cursor);
        writeRows(createNewRow(customer, 5L, "matrix"));
        expected = new RowBase[]{
            row(resultRowType, 4L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCount_DefaultCursor()
    {
        Operator plan =
            count_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                customerRowType);
        final RowType resultRowType = plan.rowType();
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(resultRowType, 3L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test
    public void testCount_TableStatusCursor()
    {
        Operator plan = count_TableStatus(customerRowType);
        final RowType resultRowType = plan.rowType();
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(resultRowType, 3L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }
}
