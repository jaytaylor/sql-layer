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
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.NegativeLimitException;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;

public class LimitIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
        NewRow[] dbRows = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(customer, 4L, "highland"),
            createNewRow(customer, 5L, "matrix"),
            createNewRow(customer, 6L, "sigma"),
            createNewRow(customer, 7L, "crv"),
        };
        use(dbRows);
    }

    // Limit tests

    @Test
    public void testLimit()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              3);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 4L, "highland"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSkip()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              2, false, Integer.MAX_VALUE, false);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
            row(customerRowType, 6L, "sigma"),
            row(customerRowType, 7L, "crv"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSkipAndLimit()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              2, false, 2, false);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSkipExhausted()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              10, false, 1, false);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testLimitFromBinding()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              0, false, 0, true);
        Cursor cursor = cursor(plan, queryContext);
        if (Types3Switch.ON)
            queryContext.setPValue(0, new PValue(MNumeric.INT.instance(false), 2));
        else
            queryContext.setValue(0, new FromObjectValueSource().setReflectively(2L));
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
        };
        compareRows(expected, cursor);
    }

    @Test(expected = NegativeLimitException.class)
    public void testLimitFromBadBinding()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              0, false, 0, true);
        Cursor cursor = cursor(plan, queryContext);
        if (Types3Switch.ON)
            queryContext.setPValue(0, new PValue(MNumeric.INT.instance(false), -1));
        else
            queryContext.setValue(0, new FromObjectValueSource().setReflectively(-1L));
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        Operator plan = limit_Default(groupScan_Default(coi), 3);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(customerRowType, 1L, "northbridge"),
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 4L, "highland"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }
}
