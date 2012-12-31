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
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.types.ValueSource;
import com.akiban.util.tap.Tap;
import com.akiban.util.tap.TapReport;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.operator.API.*;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/*
 * There are 4 usages of GroupScan_Default:
 * 1. Full scan.
 * 2. Retrieve row with hkey, without descendents.
 * 3. Retrieve row with hkey, with descendents.
 * 4. Retrieve rows satisfying range condition on hkey.
 * These correspond to the four ways in which the underlying cursor can be used, (e.g. PersistitGroupCursor).
 */

public class GroupScanIT extends OperatorITBase
{
    @Test
    public void testFullScan()
    {
        use(db);
        Operator groupScan = groupScan_Default(coi);
        Cursor cursor = cursor(groupScan, queryContext);
        RowBase[] expected = new RowBase[]{row(customerRowType, 1L, "xyz"),
                                           row(orderRowType, 11L, 1L, "ori"),
                                           row(itemRowType, 111L, 11L),
                                           row(itemRowType, 112L, 11L),
                                           row(orderRowType, 12L, 1L, "david"),
                                           row(itemRowType, 121L, 12L),
                                           row(itemRowType, 122L, 12L),
                                           row(customerRowType, 2L, "abc"),
                                           row(orderRowType, 21L, 2L, "tom"),
                                           row(itemRowType, 211L, 21L),
                                           row(itemRowType, 212L, 21L),
                                           row(orderRowType, 22L, 2L, "jack"),
                                           row(itemRowType, 221L, 22L),
                                           row(itemRowType, 222L, 22L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testFullScan_EmptyDB()
    {
        use(emptyDB);
        Operator groupScan = groupScan_Default(coi);
        Cursor cursor = cursor(groupScan, queryContext);
        compareRows(EMPTY_EXPECTED, cursor);
    }

    @Test
    public void testFindHKeyWithoutDescendents()
    {
        // A group scan locating a single hkey with its descendents is done by the AncestorLookup operator.
        // Can't test it directly here since this isn't a unit test (and we don't have access to the wrapped
        // GroupCursor or GroupCursor.rebind).
        use(db);
        IndexBound tom = orderSalesmanIndexBound("tom");
        IndexKeyRange indexKeyRange = IndexKeyRange.bounded(orderSalesmanIndexRowType, tom, true, tom, true);
        Operator groupScan = indexScan_Default(orderSalesmanIndexRowType, false, indexKeyRange);
        Operator ancestorLookup = ancestorLookup_Default(groupScan,
                                                                 coi,
                                                                 orderSalesmanIndexRowType,
                                                                 Arrays.asList(customerRowType),
                                                                 InputPreservationOption.DISCARD_INPUT);
        Cursor cursor = cursor(ancestorLookup, queryContext);
        RowBase[] expected = new RowBase[]{row(customerRowType, 2L, "abc")};
        compareRows(expected, cursor);
    }

    @Test
    public void testFindHKeyWithoutDescendents_EmptyDB()
    {
        // Testing GroupScan via AncestorLookup doesn't work for an empty database, since there won't be
        // any uses of GroupScan.
    }

    @Test
    public void testFindHKeyWithDescendents()
    {
        // A group scan locating a single hkey with its descendents is done by the Lookup operator.
        // Can't test it directly here since this isn't a unit test (and we don't have access to the wrapped
        // GroupCursor or GroupCursor.rebind).
        use(db);
        IndexBound tom = orderSalesmanIndexBound("tom");
        IndexKeyRange indexKeyRange = IndexKeyRange.bounded(orderSalesmanIndexRowType, tom, true, tom, true);
        Operator groupScan = indexScan_Default(orderSalesmanIndexRowType, false, indexKeyRange);
        Operator lookup = branchLookup_Default(groupScan, coi, orderSalesmanIndexRowType, orderRowType, InputPreservationOption.DISCARD_INPUT  );
        Cursor cursor = cursor(lookup, queryContext);
        RowBase[] expected = new RowBase[]{row(orderRowType, 21L, 2L, "tom"),
                                           row(itemRowType, 211L, 21L),
                                           row(itemRowType, 212L, 21L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testFindHKeyWithDescendents_EmptyDB()
    {
        // Testing GroupScan via Lookup doesn't work for an empty database, since there won't be
        // any uses of GroupScan.
    }
    
    // Inspired by bug 898013
    @Test
    public void testAliasingOfPersistitGroupRowFields()
    {
        use(db);
        Operator groupScan = groupScan_Default(coi);
        Cursor cursor = cursor(groupScan, queryContext);
        cursor.open();
        Row row = cursor.next();
        assertSame(customerRowType, row.rowType());
        row = cursor.next();
        assertSame(orderRowType, row.rowType());
        // Get and checking each field should work
        assertEquals(Long.valueOf(11L), getLong(row, 0));
        assertEquals(Long.valueOf(1L), getLong(row, 1));
        assertEquals("ori", row.eval(2).getString());
        // Getting all value sources and then using them should also work
        ValueSource v0 = row.eval(0);
        ValueSource v1 = row.eval(1);
        ValueSource v2 = row.eval(2);
        assertEquals(11L, v0.getInt());
        assertEquals(1L, v1.getInt());
        assertEquals("ori", v2.getString());
    }

    @Test
    public void testCursor()
    {
        use(db);
        Operator plan =
            filter_Default(
                groupScan_Default(coi),
                Collections.singleton(customerRowType));
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(customerRowType, 1L, "xyz"),
                    row(customerRowType, 2L, "abc"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private IndexBound orderSalesmanIndexBound(String salesman)
    {
        return new IndexBound(row(orderSalesmanIndexRowType, salesman), new SetColumnSelector(0));
    }

    private IndexBound customerCidIndexBound(int cid)
    {
        return new IndexBound(row(customerCidIndexRowType, cid), new SetColumnSelector(0));
    }

    /**
     * For use in HKey scan (bound contains user table rows)
     */
    private IndexBound customerCidBound(int cid)
    {
        return new IndexBound(row(customerRowType, cid, null), new SetColumnSelector(0));
    }

    private static final RowBase[] EMPTY_EXPECTED = new RowBase[]{};
}
