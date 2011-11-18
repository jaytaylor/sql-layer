/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.qp;

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.SetColumnSelector;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.qp.operator.API.*;

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
        Cursor cursor = cursor(groupScan, adapter);
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
        Cursor cursor = cursor(groupScan, adapter);
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
        IndexKeyRange indexKeyRange = new IndexKeyRange(orderSalesmanIndexRowType, tom, true, tom, true);
        Operator groupScan = indexScan_Default(orderSalesmanIndexRowType, false, indexKeyRange);
        Operator ancestorLookup = ancestorLookup_Default(groupScan,
                                                                 coi,
                                                                 orderSalesmanIndexRowType,
                                                                 Arrays.asList(customerRowType),
                                                                 LookupOption.DISCARD_INPUT);
        Cursor cursor = cursor(ancestorLookup, adapter);
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
        IndexKeyRange indexKeyRange = new IndexKeyRange(orderSalesmanIndexRowType, tom, true, tom, true);
        Operator groupScan = indexScan_Default(orderSalesmanIndexRowType, false, indexKeyRange);
        Operator lookup = branchLookup_Default(groupScan, coi, orderSalesmanIndexRowType, orderRowType, LookupOption.DISCARD_INPUT  );
        Cursor cursor = cursor(lookup, adapter);
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
