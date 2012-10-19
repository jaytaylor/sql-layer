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

import com.akiban.ais.model.*;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Comparison;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.server.test.ExpressionGenerators.*;
import static com.akiban.qp.operator.API.*;
import static com.akiban.qp.operator.API.JoinType.*;

public class OperatorIT extends OperatorITBase
{
    @Before
    public void before()
    {
        super.before();
        use(db);
    }

    @Test
    public void testGroupScan() throws Exception
    {
        Operator groupScan = groupScan_Default(coi);
        Cursor executable = cursor(groupScan, queryContext);
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
        compareRows(expected, executable);
    }

    @Test
    public void testSelect()
    {
        Operator groupScan = groupScan_Default(coi);
        ExpressionGenerator cidEq2 = compare(field(customerRowType, 0), Comparison.EQ, literal(2L));
        Operator select = select_HKeyOrdered(groupScan, customerRowType, cidEq2);
        RowBase[] expected = new RowBase[]{row(customerRowType, 2L, "abc"),
                                           row(orderRowType, 21L, 2L, "tom"),
                                           row(itemRowType, 211L, 21L),
                                           row(itemRowType, 212L, 21L),
                                           row(orderRowType, 22L, 2L, "jack"),
                                           row(itemRowType, 221L, 22L),
                                           row(itemRowType, 222L, 22L)};
        compareRows(expected, cursor(select, queryContext));
    }

    @Test
    public void testFlatten()
    {
        Operator groupScan = groupScan_Default(coi);
        Operator flatten = flatten_HKeyOrdered(groupScan, customerRowType, orderRowType, INNER_JOIN);
        RowType flattenType = flatten.rowType();
        RowBase[] expected = new RowBase[]{row(flattenType, 1L, "xyz", 11L, 1L, "ori"),
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
        compareRows(expected, cursor(flatten, queryContext));
    }

    @Test
    public void testTwoFlattens()
    {
        Operator groupScan = groupScan_Default(coi);
        Operator flattenCO = flatten_HKeyOrdered(groupScan, customerRowType, orderRowType, INNER_JOIN);
        Operator flattenCOI = flatten_HKeyOrdered(flattenCO, flattenCO.rowType(), itemRowType, INNER_JOIN);
        RowType flattenCOIType = flattenCOI.rowType();
        RowBase[] expected = new RowBase[]{row(flattenCOIType, 1L, "xyz", 11L, 1L, "ori", 111L, 11L),
                                           row(flattenCOIType, 1L, "xyz", 11L, 1L, "ori", 112L, 11L),
                                           row(flattenCOIType, 1L, "xyz", 12L, 1L, "david", 121L, 12L),
                                           row(flattenCOIType, 1L, "xyz", 12L, 1L, "david", 122L, 12L),
                                           row(flattenCOIType, 2L, "abc", 21L, 2L, "tom", 211L, 21L),
                                           row(flattenCOIType, 2L, "abc", 21L, 2L, "tom", 212L, 21L),
                                           row(flattenCOIType, 2L, "abc", 22L, 2L, "jack", 221L, 22L),
                                           row(flattenCOIType, 2L, "abc", 22L, 2L, "jack", 222L, 22L)};
        compareRows(expected, cursor(flattenCOI, queryContext));
    }

    @Test
    public void testIndexScan1()
    {
        Operator indexScan = indexScan_Default(indexType(customer, "name"));
        // TODO: Can't compare rows, because we can't yet obtain fields from index rows. So compare hkeys instead
        String[] expected = new String[]{"{1,(long)2}",
                                         "{1,(long)1}"};
        compareRenderedHKeys(expected, cursor(indexScan, queryContext));
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
        compareRenderedHKeys(expected, cursor(indexScan, queryContext));
    }

    @Test
    public void testIndexLookup()
    {
        Operator indexScan = indexScan_Default(indexType(order, "salesman"));
        Operator lookup = branchLookup_Default(indexScan, coi, orderSalesmanIndexRowType, orderRowType, InputPreservationOption.DISCARD_INPUT);
        RowBase[] expected = new RowBase[]{row(orderRowType, 12L, 1L, "david"),
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
        compareRows(expected, cursor(lookup, queryContext));
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
        RowBase[] expected = new RowBase[]{row(customerRowType, 1L, "xyz"),
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
        compareRows(expected, cursor(ancestorLookup, queryContext));
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
        RowBase[] expected = new RowBase[]{row(customerRowType, 1L, "xyz"),
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
        compareRows(expected, cursor(ancestorLookup, queryContext));
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
        compareRenderedHKeys(expected, cursor(indexScan, queryContext));
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
        RowBase[] expected = new RowBase[]{row(orderRowType, 21L, 2L, "tom"),
                                           row(itemRowType, 211L, 21L),
                                           row(itemRowType, 212L, 21L)};
        compareRows(expected, cursor(lookup, queryContext));

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
        RowBase[] expected = new RowBase[]{row(customerRowType, 2L, "abc")};
        compareRows(expected, cursor(ancestorLookup, queryContext));
    }
}
