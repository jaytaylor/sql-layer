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

import com.foundationdb.qp.expression.ExpressionRow;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.expression.RowBasedUnboundExpressions;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.*;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class IndexScanLookaheadIT extends OperatorITBase
{
    @Override
    protected boolean pipelineMap() {
        return true;
    }

    protected int lookaheadQuantum() {
        return 4;
    }

    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        // Like super, but with a whole lot more cid=2 orders.
        db = new Row[]{ row(customer, 1L, "xyz"),
                          row(customer, 2L, "abc"),
                          row(customer, 4L, "qqq"),
                          row(order, 11L, 1L, "ori"),
                          row(order, 12L, 1L, "david"),
                          row(order, 21L, 2L, "tom"),
                          row(order, 22L, 2L, "jack"),
                          row(order, 23L, 2L, "dave"),
                          row(order, 24L, 2L, "dave"),
                          row(order, 25L, 2L, "dave"),
                          row(order, 26L, 2L, "dave"),
                          row(order, 27L, 2L, "dave"),
                          row(order, 28L, 2L, "dave"),
                          row(order, 29L, 2L, "dave"),
                          row(order, 41L, 4L, "nathan"),
                          row(item, 111L, 11L),
                          row(item, 112L, 11L),
                          row(item, 121L, 12L),
                          row(item, 122L, 12L),
                          row(item, 211L, 21L),
                          row(item, 212L, 21L),
                          row(item, 221L, 22L),
                          row(item, 222L, 22L),
                          row(item, 241L, 24L),
                          row(item, 251L, 25L)};
        use(db);
    }

    @Test
    public void testCursor()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, iidKeyRange(100, false, 125, false), ordering(itemIidIndexRowType), IndexScanSelector.leftJoinAfter(itemIidIndexRowType.index(), itemRowType.table()), lookaheadQuantum());
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public boolean hKeyComparison()
            {
                return true;
            }

            @Override
            public String[] firstExpectedHKeys()
            {
                return new String[]{hkey(1, 11, 111), hkey(1, 11, 112), hkey(1, 12, 121), hkey(1, 12, 122)};
            }
        };
        testCursorLifecycle(indexScan, testCase);
    }

    @Test
    public void testSingle()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, iidKeyRange(212, true, 212, true), ordering(itemIidIndexRowType), IndexScanSelector.leftJoinAfter(itemIidIndexRowType.index(), itemRowType.table()), lookaheadQuantum());
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 212)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testMap()
    {
        RowType cidValueRowType = schema.newValuesType(MNumeric.INT.instance(true));
        List<ExpressionGenerator> cidExprs = Arrays.asList(boundField(cidValueRowType, 1, 0));
        IndexBound cidBound =
            new IndexBound(
                new RowBasedUnboundExpressions(orderCidIndexRowType, cidExprs, true),
                new SetColumnSelector(0));
        IndexKeyRange cidRange = IndexKeyRange.bounded(orderCidIndexRowType, cidBound, true, cidBound, true);
        Operator plan =
            map_NestedLoops(
                valuesScan_Default(
                    bindableExpressions(intRow(cidValueRowType, 2),
                                        intRow(cidValueRowType, 4),
                                        intRow(cidValueRowType, 6)),
                    cidValueRowType),
                indexScan_Default(orderCidIndexRowType, cidRange, ordering(orderCidIndexRowType), IndexScanSelector.leftJoinAfter(orderCidIndexRowType.index(), orderRowType.table()), lookaheadQuantum()),
                1, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21),hkey(2, 22),hkey(2, 23),hkey(2, 24),hkey(2, 25),hkey(2, 26),hkey(2, 27),hkey(2, 28),hkey(2, 29),hkey(4, 41)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testNested()
    {
        RowType cidValueRowType = schema.newValuesType(MNumeric.INT.instance(true));
        List<ExpressionGenerator> cidExprs = Arrays.asList(boundField(cidValueRowType, 1, 0));
        IndexBound cidBound =
            new IndexBound(
                new RowBasedUnboundExpressions(orderCidIndexRowType, cidExprs, true),
                new SetColumnSelector(0));
        IndexKeyRange cidRange = IndexKeyRange.bounded(orderCidIndexRowType, cidBound, true, cidBound, true);
        List<ExpressionGenerator> oidExprs = Arrays.asList(boundField(orderCidIndexRowType, 2, 1));
        IndexBound oidBound =
            new IndexBound(
                new RowBasedUnboundExpressions(itemOidIndexRowType, oidExprs, true),
                new SetColumnSelector(0));
        IndexKeyRange oidRange = IndexKeyRange.bounded(itemOidIndexRowType, oidBound, true, oidBound, true);
        Operator plan =
            map_NestedLoops(
                valuesScan_Default(
                    bindableExpressions(intRow(cidValueRowType, 2),
                                        intRow(cidValueRowType, 4),
                                        intRow(cidValueRowType, 6)),
                    cidValueRowType),
                map_NestedLoops(
                    indexScan_Default(orderCidIndexRowType, cidRange, ordering(orderCidIndexRowType), IndexScanSelector.leftJoinAfter(orderCidIndexRowType.index(), orderRowType.table()), lookaheadQuantum()),
                    indexScan_Default(itemOidIndexRowType, oidRange, ordering(itemOidIndexRowType), IndexScanSelector.leftJoinAfter(itemOidIndexRowType.index(), itemRowType.table()), lookaheadQuantum()),
                    2, pipelineMap(), 2),
                1, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 211),hkey(2, 21, 212),hkey(2, 22, 221),hkey(2, 22, 222),hkey(2, 24, 241),hkey(2, 25, 251)};
        compareRenderedHKeys(expected, cursor);
    }

    // For use by this class

    private IndexKeyRange iidKeyRange(int lo, boolean loInclusive, int hi, boolean hiInclusive)
    {
        return IndexKeyRange.bounded(itemIidIndexRowType, iidBound(lo), loInclusive, iidBound(hi), hiInclusive);
    }

    private IndexBound iidBound(int iid)
    {
        return new IndexBound(row(itemIidIndexRowType, iid, null, null), new SetColumnSelector(0));
    }

    private Row intRow(RowType rowType, int x)
    {
        List<TPreparedExpression> pExpressions = Arrays.<TPreparedExpression>asList(new TPreparedLiteral(MNumeric.INT.instance(false), new Value(MNumeric.INT.instance(false), x)));
        return new ExpressionRow(rowType, queryContext, queryBindings, pExpressions);
    }

    private Collection<? extends BindableRow> bindableExpressions(Row... rows) {
        List<BindableRow> result = new ArrayList<>();
        for (Row row : rows) {
            result.add(BindableRow.of(row));
        }
        return result;
    }

    private String hkey(int cid, int oid)
    {
        return String.format("{1,(long)%s,2,(long)%s}", cid, oid);
    }

    private String hkey(int cid, int oid, int iid)
    {
        return String.format("{1,(long)%s,2,(long)%s,3,(long)%s}", cid, oid, iid);
    }

    private Ordering ordering(IndexRowType indexRowType) {
        Ordering ordering = new Ordering();
        for (int i = 0; i < indexRowType.nFields(); i++) {
            ordering.append(field(indexRowType, i), true);
        }
        return ordering;
    }

}
