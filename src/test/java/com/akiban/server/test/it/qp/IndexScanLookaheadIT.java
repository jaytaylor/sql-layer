/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.test.it.qp;

import com.akiban.qp.expression.ExpressionRow;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedLiteral;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.*;

import org.junit.Test;
import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexScanLookaheadIT extends OperatorITBase
{
    @Override
    protected Map<String, String> startupConfigProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("akserver.lookaheadQuantum.indexScan", "4");
        return properties;
    }

    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        use(db);
    }

    @Test
    public void testCursor()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, false, 125, false));
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
                return new String[]{hkey(1, 12, 122), hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
            }
        };
        testCursorLifecycle(indexScan, testCase);
    }

    @Test
    public void testSingle()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(212, true, 212, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 212)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testMap()
    {
        RowType cidValueRowType = schema.newValuesType(AkType.INT);
        List<ExpressionGenerator> cidExprs = Arrays.asList(boundField(cidValueRowType, 1, 0));
        IndexBound cidBound =
            new IndexBound(
                new RowBasedUnboundExpressions(orderCidIndexRowType, cidExprs),
                new SetColumnSelector(0));
        IndexKeyRange cidRange = IndexKeyRange.bounded(orderCidIndexRowType, cidBound, true, cidBound, true);
        Operator plan =
            map_NestedLoops(
                valuesScan_Default(
                    bindableExpressions(intRow(cidValueRowType, 2),
                                        intRow(cidValueRowType, 4),
                                        intRow(cidValueRowType, 6)),
                    cidValueRowType),
                 indexScan_Default(orderCidIndexRowType, false, cidRange),
                1, 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21),hkey(2, 22)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testNested()
    {
        RowType cidValueRowType = schema.newValuesType(AkType.INT);
        List<ExpressionGenerator> cidExprs = Arrays.asList(boundField(cidValueRowType, 1, 0));
        IndexBound cidBound =
            new IndexBound(
                new RowBasedUnboundExpressions(orderCidIndexRowType, cidExprs),
                new SetColumnSelector(0));
        IndexKeyRange cidRange = IndexKeyRange.bounded(orderCidIndexRowType, cidBound, true, cidBound, true);
        List<ExpressionGenerator> oidExprs = Arrays.asList(boundField(orderCidIndexRowType, 2, 1));
        IndexBound oidBound =
            new IndexBound(
                new RowBasedUnboundExpressions(itemOidIndexRowType, oidExprs),
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
                    indexScan_Default(orderCidIndexRowType, false, cidRange),
                    indexScan_Default(itemOidIndexRowType, false, oidRange),
                    2, 2),
                1, 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 211),hkey(2, 21, 212),hkey(2, 22,221),hkey(2, 22,222)};
        compareRenderedHKeys(expected, cursor);
    }

    // For use by this class

    private IndexKeyRange iidKeyRange(int lo, boolean loInclusive, int hi, boolean hiInclusive)
    {
        return IndexKeyRange.bounded(itemIidIndexRowType, iidBound(lo), loInclusive, iidBound(hi), hiInclusive);
    }

    private IndexBound iidBound(int iid)
    {
        return new IndexBound(row(itemIidIndexRowType, iid), new SetColumnSelector(0));
    }

    private Row intRow(RowType rowType, int x)
    {
        List<TPreparedExpression> pExpressions = Arrays.<TPreparedExpression>asList(new TPreparedLiteral(MNumeric.INT.instance(false), new PValue(MNumeric.INT.instance(false), x)));
        return new ExpressionRow(rowType, queryContext, queryBindings, null, pExpressions);
    }

    private Collection<? extends BindableRow> bindableExpressions(Row... rows) {
        List<BindableRow> result = new ArrayList<>();
        for (Row row : rows) {
            result.add(BindableRow.of(row, true));
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

}
