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
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.expression.std.FieldExpression;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static com.akiban.qp.operator.API.*;

public class IndexScanProfilePT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        t = createTable(
            "schema", "t",
            "c1 int",
            "c2 int",
            "c3 int",
            "c4 int",
            "c5 int",
            "id int not null key",
            "index(c1, c2, c3, c4, c5)");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "c1", "c2", "c3", "c4", "c5");
        group = groupTable(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    @Test
    public void profileGroupScan()
    {
        final int ROWS = 1000;
        final int SCANS = 1000000;
        populateDB(ROWS);
        IndexBound location = new IndexBound(row(idxRowType, 1, null, null, null, null), new SetColumnSelector(0));
        Ordering ordering = new Ordering();
        ordering.append(new FieldExpression(idxRowType, 0), false);
        ordering.append(new FieldExpression(idxRowType, 1), false);
/*
        ordering.append(new FieldExpression(idxRowType, 2), false); // true);
        ordering.append(new FieldExpression(idxRowType, 3), false); // true);
        ordering.append(new FieldExpression(idxRowType, 4), false); // true);
*/
        IndexKeyRange keyRange = IndexKeyRange.bounded(idxRowType, location, true, location, true);
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        for (int s = 0; s < SCANS; s++) {
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            while (cursor.next() != null) {
            }
            cursor.close();
        }
    }

    protected void populateDB(int rows)
    {
        Random random = new Random();
        for (int id = 0; id < rows; id++) {
            int c1 = random.nextInt(3); // location id
            int c2 = random.nextInt(Integer.MAX_VALUE); // last login
            int c3 = 0; // abuse
            int c4 = random.nextInt(Integer.MAX_VALUE); // primaryPic
            int c5 = random.nextInt(Integer.MAX_VALUE); // profileID
            dml().writeRow(session(), createNewRow(t, c1, c2, c3, c4, c5, id));
        }
    }

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private GroupTable group;
}
