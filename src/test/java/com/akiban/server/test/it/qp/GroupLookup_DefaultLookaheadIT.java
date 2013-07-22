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
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedLiteral;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.*;

public class GroupLookup_DefaultLookaheadIT extends GroupLookup_DefaultIT
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        NewRow[] dbWithOrphans = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(order, 11L, 1L, "ori"),
            createNewRow(order, 12L, 1L, "david"),
            createNewRow(order, 21L, 2L, "tom"),
            createNewRow(order, 22L, 2L, "jack"),
            createNewRow(order, 23L, 2L, "dave"),
            createNewRow(order, 24L, 2L, "dave"),
            createNewRow(order, 25L, 2L, "dave"),
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

    @Override
    protected boolean pipelineMap() {
        return true;
    }

    @Override
    protected int lookaheadQuantum() {
        return 4;
    }

    @Test
    public void testAncestorLookupCursor()
    {
        Operator plan =
            groupLookup_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                coi,
                orderRowType,
                Collections.singleton(customerRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(customerRowType, 1L, "northbridge"),
                    row(customerRowType, 1L, "northbridge"),
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 2L, "foundation"),
                };
            }

            @Override
            public boolean reopenTopLevel() {
                return true;
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test @Ignore // Same check in testCursor
    public void testAncestorLookupSimple()
    {
        Operator plan =
            groupLookup_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                coi,
                orderRowType,
                Collections.singleton(customerRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 2L, "foundation"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testAncestorLookupMap()
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
                    bindableExpressions(intRow(cidValueRowType, 3),
                                        intRow(cidValueRowType, 2),
                                        intRow(cidValueRowType, 10)),
                    cidValueRowType),
                groupLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidRange, ordering(orderCidIndexRowType), IndexScanSelector.leftJoinAfter(orderCidIndexRowType.index(), orderRowType.userTable()), lookaheadQuantum()),
                    coi,
                    orderCidIndexRowType,
                    Arrays.asList(customerRowType, orderRowType),
                    InputPreservationOption.DISCARD_INPUT,
                    lookaheadQuantum()),
                1, pipelineMap(), 1);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 22L, 2L, "jack"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 23L, 2L, "dave"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 24L, 2L, "dave"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 25L, 2L, "dave"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    // For use by this class

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

    private Ordering ordering(IndexRowType indexRowType) {
        Ordering ordering = new Ordering();
        for (int i = 0; i < indexRowType.nFields(); i++) {
            ordering.append(field(indexRowType, i), true);
        }
        return ordering;
    }

}
