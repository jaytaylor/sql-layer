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

package com.akiban.server.test.pt.qp;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.util.tap.Tap;
import com.akiban.util.tap.TapReport;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;

public class SortProfilePT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        t = createTable(
            "schema", "t",
            "id int not null key",
            "name varchar(20)");
        group = groupTable(t);
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        adapter = persistitAdapter(schema);
        queryContext = queryContext((PersistitAdapter) adapter);
    }

    @Test
    public void profileSort()
    {
        final int N = 100000;
        Ordering ordering = ordering();
        ordering.append(new FieldExpression(tRowType, 0), true);
        Operator plan = sort_Tree(groupScan_Default(group), tRowType, ordering, SortOption.PRESERVE_DUPLICATES);
        Tap.setEnabled(ALL_TAPS, true);
        for (int id = 0; id < N; id++) {
            dml().writeRow(session(), createNewRow(t, id, String.format("%s", id)));
        }
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();
        while (cursor.next() != null) {
        }
        cursor.close();
        TapReport[] reports = Tap.getReport(ALL_TAPS);
        for (TapReport report : reports) {
            System.out.println(report);
        }
    }
    
    private static final String ALL_TAPS = ".*";

    private int t;
    private GroupTable group;
    private Schema schema;
    private RowType tRowType;
    private StoreAdapter adapter;
}
