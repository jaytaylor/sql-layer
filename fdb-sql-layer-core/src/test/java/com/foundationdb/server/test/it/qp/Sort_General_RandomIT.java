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

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.types.value.ValueSources;
import org.junit.Test;

import java.util.*;

import static com.foundationdb.server.test.ExpressionGenerators.field;
import static com.foundationdb.qp.operator.API.*;

// More sort_General testing, with randomly generated data

public class Sort_General_RandomIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        // Don't call super.before(). This is a different schema from most operator ITs.
        t = createTable(
            "schema", "t",
            "a int not null",
            "b int not null",
            "c int not null",
            "d int not null",
            "id int not null primary key");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        group = group(t);
        List<Row> rows = new ArrayList<>();
        Random random = new Random(123456789);
        long key = 0;
        for (long a = 0; a < A; a++) {
            int nB = random.nextInt(R) + 1;
            for (long b = 0; b < nB; b++) {
                int nC = random.nextInt(R) + 1;
                for (long c = 0; c < nC; c++) {
                    int nD = random.nextInt(R) + 1;
                    for (long d = 0; d < nD; d++) {
                        Row row = row(t, a, b, c, d, key++);
                        rows.add(row);
                    }
                }
            }
        }
        db = new Row[rows.size()];
        rows.toArray(db);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    @Test
    public void testSort()
    {
        for (int x = 0; x < 16; x++) {
            boolean aAsc = (x & 8) != 0;
            boolean bAsc = (x & 4) != 0;
            boolean cAsc = (x & 2) != 0;
            boolean dAsc = (x & 1) != 0;
            Operator plan =
                sort_General(
                    groupScan_Default(group),
                    tRowType,
                    ordering(field(tRowType, 0), aAsc, field(tRowType, 1), bAsc, field(tRowType, 2), cAsc, field(tRowType, 3), dAsc),
                    SortOption.PRESERVE_DUPLICATES);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            compareRows(expected(aAsc, bAsc, cAsc, dAsc), cursor);
        }
    }

    private Row[] expected(final boolean ... asc)
    {
        Row[] sorted = new Row[db.length];
        Comparator<Row> comparator =
            new Comparator<Row>()
            {
                @Override
                public int compare(Row x, Row y)
                {
                    int c = 0;
                    for (int i = 0; c == 0 && i < 4; i++) {
                        c = compare(x, y, asc, i);
                    }
                    return c;
                }

                private int compare(Row x, Row y, boolean[] asc, int i)
                {
                    return (int) ((ValueSources.getLong(x.value(i)) - ValueSources.getLong(y.value(i)))) * (asc[i] ? 1 : -1);
                }
            };
        Arrays.sort(db, comparator);
        int r = 0;
        for (Row dbRow : db) {
            sorted[r++] = dbRow;
        }
        return sorted;
    }

    private Ordering ordering(Object... objects)
    {
        Ordering ordering = API.ordering();
        int i = 0;
        while (i < objects.length) {
            ExpressionGenerator expression = (ExpressionGenerator) objects[i++];
            Boolean ascending = (Boolean) objects[i++];
            ordering.append(expression, ascending);
        }
        return ordering;
    }

    private static final int A = 100; // Number of distinct t.a values
    private static final int R = 3; // Maximum number of t.b values per a, c values per b, d values per c

    private int t;
    private RowType tRowType;
    private Group group;
}
