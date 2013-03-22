
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
