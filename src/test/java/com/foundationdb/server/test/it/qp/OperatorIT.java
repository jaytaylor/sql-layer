/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.test.it.qp;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.texpressions.Comparison;
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.server.test.ExpressionGenerators.*;
import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.qp.operator.API.JoinType.*;

public class OperatorIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
        use(db);
    }

    @Test
    public void testGroupScan() throws Exception
    {
        Operator groupScan = groupScan_Default(coi);
        Cursor executable = cursor(groupScan, queryContext, queryBindings);
        Row[] expected = new Row[]{row(customerRowType, 1L, "xyz"),
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
        compareRows(expected, executable);
    }

    @Test
    public void testSelect()
    {
        Operator groupScan = groupScan_Default(coi);
        ExpressionGenerator cidEq2 = compare(field(customerRowType, 0), Comparison.EQ, literal(2L), castResolver());
        Operator select = select_HKeyOrdered(groupScan, customerRowType, cidEq2);
        Row[] expected = new Row[]{row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L)};
        compareRows(expected, cursor(select, queryContext, queryBindings));
    }

    @Test
    public void testFlatten()
    {
        Operator groupScan = groupScan_Default(coi);
        Operator flatten = flatten_HKeyOrdered(groupScan, customerRowType, orderRowType, INNER_JOIN);
        RowType flattenType = flatten.rowType();
        Row[] expected = new Row[]{row(flattenType, 1L, "xyz", 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(itemRowType, 112L, 11L),
                                   row(flattenType, 1L, "xyz", 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(itemRowType, 122L, 12L),
                                   row(flattenType, 2L, "abc", 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L),
                                   row(flattenType, 2L, "abc", 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L)};
        compareRows(expected, cursor(flatten, queryContext, queryBindings));
    }

    @Test
    public void testTwoFlattens()
    {
        Operator groupScan = groupScan_Default(coi);
        Operator flattenCO = flatten_HKeyOrdered(groupScan, customerRowType, orderRowType, INNER_JOIN);
        Operator flattenCOI = flatten_HKeyOrdered(flattenCO, flattenCO.rowType(), itemRowType, INNER_JOIN);
        RowType flattenCOIType = flattenCOI.rowType();
        Row[] expected = new Row[]{row(flattenCOIType, 1L, "xyz", 11L, 1L, "ori", 111L, 11L),
                                   row(flattenCOIType, 1L, "xyz", 11L, 1L, "ori", 112L, 11L),
                                   row(flattenCOIType, 1L, "xyz", 12L, 1L, "david", 121L, 12L),
                                   row(flattenCOIType, 1L, "xyz", 12L, 1L, "david", 122L, 12L),
                                   row(flattenCOIType, 2L, "abc", 21L, 2L, "tom", 211L, 21L),
                                   row(flattenCOIType, 2L, "abc", 21L, 2L, "tom", 212L, 21L),
                                   row(flattenCOIType, 2L, "abc", 22L, 2L, "jack", 221L, 22L),
                                   row(flattenCOIType, 2L, "abc", 22L, 2L, "jack", 222L, 22L)};
        compareRows(expected, cursor(flattenCOI, queryContext, queryBindings));
    }

    @Test
    public void testIndexScan1()
    {
        Operator indexScan = indexScan_Default(indexType(customer, "name"));
        // TODO: Can't compare rows, because we can't yet obtain fields from index rows. So compare hkeys instead
        String[] expected = new String[]{"{1,(long)2}",
                                         "{1,(long)1}"};
        compareRenderedHKeys(expected, cursor(indexScan, queryContext, queryBindings));
    }

    @Test
    public void testIndexScan2()
    {
        Operator indexScan = indexScan_Default(indexType(order, "salesman"));
        // TODO: Can't compare rows, because we can't yet obtain fields from index rows. So compare hkeys instead
        String[] expected = new String[]{"{1,(long)1,2,(long)12}",
                                         "{1,(long)2,2,(long)22}",
                                         "{1,(long)1,2,(long)11}",
                                         "{1,(long)2,2,(long)21}"};
        compareRenderedHKeys(expected, cursor(indexScan, queryContext, queryBindings));
    }

    @Test
    public void testIndexLookup()
    {
        Operator indexScan = indexScan_Default(indexType(order, "salesman"));
        Operator lookup = branchLookup_Default(indexScan, coi, orderSalesmanIndexRowType, orderRowType, InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[]{row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(itemRowType, 122L, 12L),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(itemRowType, 112L, 11L),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L)};
        compareRows(expected, cursor(lookup, queryContext, queryBindings));
    }

    @Test
    public void testIndexLookupWithOneAncestor()
    {
        Operator indexScan = indexScan_Default(indexType(order, "salesman"));
        Operator lookup = branchLookup_Default(indexScan, coi, orderSalesmanIndexRowType, orderRowType, InputPreservationOption.DISCARD_INPUT);
        Operator ancestorLookup = ancestorLookup_Default(lookup,
                                                                 coi,
                                                                 orderRowType,
                                                                 Arrays.asList(customerRowType),
                                                                 InputPreservationOption.KEEP_INPUT);
        Row[] expected = new Row[]{row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(itemRowType, 122L, 12L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L),
                                   row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(itemRowType, 112L, 11L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L)};
        compareRows(expected, cursor(ancestorLookup, queryContext, queryBindings));
    }

    @Test
    public void testIndexLookupWithTwoAncestors()
    {
        Operator indexScan = indexScan_Default(indexType(item, "oid"));
        Operator lookup = branchLookup_Default(indexScan,
                                                       coi,
                                                       itemOidIndexRowType,
                                                       itemRowType,
                                                       InputPreservationOption.DISCARD_INPUT);
        Operator ancestorLookup = ancestorLookup_Default(lookup,
                                                                 coi,
                                                                 itemRowType,
                                                                 Arrays.asList(customerRowType, orderRowType),
                                                                 InputPreservationOption.KEEP_INPUT);
        Row[] expected = new Row[]{row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 112L, 11L),
                                   row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 122L, 12L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 212L, 21L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 222L, 22L)};
        compareRows(expected, cursor(ancestorLookup, queryContext, queryBindings));
    }

    @Test
    public void testRestrictedIndexScan()
    {
        Index idxOrderSalesman = orderSalesmanIndexRowType.index();
        IndexBound lo = indexBound(row(orderSalesmanIndexRowType, "jack"), columnSelector(idxOrderSalesman));
        IndexBound hi = indexBound(row(orderSalesmanIndexRowType, "tom"), columnSelector(idxOrderSalesman));
        IndexKeyRange range = indexKeyRange(orderSalesmanIndexRowType, lo, true, hi, false);
        Operator indexScan = indexScan_Default(orderSalesmanIndexRowType, false, range);
        // TODO: Can't compare rows, because we can't yet obtain fields from index rows. So compare hkeys instead
        String[] expected = new String[]{"{1,(long)2,2,(long)22}",
                                         "{1,(long)1,2,(long)11}"};
        compareRenderedHKeys(expected, cursor(indexScan, queryContext, queryBindings));
    }

    @Test
    public void testRestrictedIndexLookup()
    {
        Index idxOrderSalesman = orderSalesmanIndexRowType.index();
        IndexBound tom = indexBound(row(orderSalesmanIndexRowType, "tom"), columnSelector(idxOrderSalesman));
        IndexKeyRange matchTom = indexKeyRange(orderSalesmanIndexRowType, tom, true, tom, true);
        Operator indexScan = indexScan_Default(orderSalesmanIndexRowType, false, matchTom);
        Operator lookup = branchLookup_Default(indexScan,
                                                       coi,
                                                       orderSalesmanIndexRowType,
                                                       orderRowType,
                                                       InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[]{row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L)};
        compareRows(expected, cursor(lookup, queryContext, queryBindings));

    }

    @Test
    public void testAncestorLookupAfterIndexScan()
    {
        // Find customers associated with salesman tom
        Index idxOrderSalesman = orderSalesmanIndexRowType.index();
        IndexBound tom = indexBound(row(orderSalesmanIndexRowType, "tom"), columnSelector(idxOrderSalesman));
        IndexKeyRange matchTom = indexKeyRange(orderSalesmanIndexRowType, tom, true, tom, true);
        Operator indexScan = indexScan_Default(orderSalesmanIndexRowType, false, matchTom);
        Operator ancestorLookup = ancestorLookup_Default(indexScan,
                                                                 coi,
                                                                 orderSalesmanIndexRowType,
                                                                 Arrays.asList(customerRowType),
                                                                 InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[]{row(customerRowType, 2L, "abc")};
        compareRows(expected, cursor(ancestorLookup, queryContext, queryBindings));
    }
}
