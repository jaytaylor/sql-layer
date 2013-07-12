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

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.akiban.qp.operator.API.*;

public class GroupLookupLookaheadIT extends OperatorITBase
{
    @Override
    protected Map<String, String> startupConfigProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("akserver.lookaheadQuantum.groupLookup", "4");
        return properties;
    }

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

    @Test
    public void testCursor()
    {
        Operator plan =
            ancestorLookup_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                coi,
                orderRowType,
                Collections.singleton(customerRowType),
                InputPreservationOption.DISCARD_INPUT);
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
                };
            }

            @Override
            public boolean reopenTopLevel() {
                return true;
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test
    public void testSimple()
    {
        Operator plan =
            ancestorLookup_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                coi,
                orderRowType,
                Collections.singleton(customerRowType),
                InputPreservationOption.DISCARD_INPUT);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 2L, "foundation"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

}
