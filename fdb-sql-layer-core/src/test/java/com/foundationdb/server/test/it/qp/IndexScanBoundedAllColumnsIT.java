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

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class IndexScanBoundedAllColumnsIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
                "schema", "t",
                "id int not null primary key",
                "a int",
                "b int",
                "c int");
        createIndex("schema", "t", "a", "a", "b", "c", "id");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new Row[]{
                // No nulls
                row(t, 1000L, 1L, 11L, 111L),
                row(t, 1001L, 1L, 11L, 115L),
                row(t, 1002L, 1L, 15L, 151L),
                row(t, 1003L, 1L, 15L, 155L),
                row(t, 1004L, 5L, 51L, 511L),
                row(t, 1005L, 5L, 51L, 515L),
                row(t, 1006L, 5L, 55L, 551L),
                row(t, 1007L, 5L, 55L, 555L),
                row(t, 1008L, 5L, 55L, 555L),
                row(t, 1009L, 5L, 55L, 555L),
        };
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }


    @Test
    public void testBounded_all4columns() {

        // DADD
        test(range(INCLUSIVE, 5, 55, 555, null,
                        INCLUSIVE, 5, 55, 555, 1009),
                ordering(DESC, ASC, DESC, DESC),
                1009, 1008, 1007
        );
        test(range(EXCLUSIVE, 5, 55, 555, null,
                        EXCLUSIVE, 5, 55, 555, 1009),
                ordering(DESC, ASC, DESC, DESC),
                1008, 1007
        );
        test(range(EXCLUSIVE, 5, 55, 555, 1007,
                        EXCLUSIVE, 5, 55, 555, null),
                ordering(DESC, ASC, DESC, DESC),
                1009, 1008
        );

        // DAAD
        test(range(INCLUSIVE, 5, 55, 555, null,
                        INCLUSIVE, 5, 55, 555, 1009),
                ordering(DESC, ASC, ASC, DESC),
                1009, 1008, 1007
        );
        test(range(EXCLUSIVE, 5, 55, 555, null,
                        EXCLUSIVE, 5, 55, 555, 1009),
                ordering(DESC, ASC, ASC, DESC),
                1008, 1007
        );
        test(range(EXCLUSIVE, 5, 55, 555, 1007,
                        EXCLUSIVE, 5, 55, 555, null),
                ordering(DESC, ASC, ASC, DESC),
                1009, 1008
        );
    }

    // For use by this class

    private void test(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        Row[] expected = new Row[expectedIds.length];
        for (int i = 0; i < expectedIds.length; i++) {
            int id = expectedIds[i];
            expected[i] = dbRow(id);
        }
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private void dump(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        dumpToAssertion(plan);
    }

    private IndexKeyRange range(boolean loInclusive, Integer aLo, Integer bLo, Integer cLo, Integer idLo,
                                boolean hiInclusive, Integer aHi, Integer bHi, Integer cHi, Integer idHi)
    {
        IndexBound lo;
        if (aLo == UNSPECIFIED) {
            lo = null;
            fail();
        } else if (bLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo), new SetColumnSelector(0));
        } else if (cLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo, bLo), new SetColumnSelector(0, 1));
        } else if (idLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo, bLo, cLo), new SetColumnSelector(0, 1, 2));
        }  else {
            lo = new IndexBound(row(idxRowType, aLo, bLo, cLo, idLo), new SetColumnSelector(0, 1, 2, 3));
        }
        IndexBound hi;
        if (aHi == UNSPECIFIED) {
            hi = null;
            fail();
        } else if (bHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi), new SetColumnSelector(0));
        } else if (cHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi, bHi), new SetColumnSelector(0, 1));
        } else if (idHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi, bHi, cHi), new SetColumnSelector(0, 1, 2));
        } else {
            hi = new IndexBound(row(idxRowType, aHi, bHi, cHi, idHi), new SetColumnSelector(0, 1, 2, 3));
        }
        return IndexKeyRange.bounded(idxRowType, lo, loInclusive, hi, hiInclusive);
    }


    private API.Ordering ordering(boolean... directions)
    {
        assertTrue(directions.length >= 1 && directions.length <= 4);
        API.Ordering ordering = API.ordering();
        if (directions.length >= 1) {
            ordering.append(field(idxRowType, A), directions[0]);
        }
        if (directions.length >= 2) {
            ordering.append(field(idxRowType, B), directions[1]);
        }
        if (directions.length >= 3) {
            ordering.append(field(idxRowType, C), directions[2]);
        }
        if (directions.length >= 4) {
            ordering.append(field(idxRowType, C), directions[3]);
        }
        return ordering;
    }

    private Row dbRow(long id)
    {
        for (Row newRow : db) {
            if (ValueSources.getLong(newRow.value(0)) == id) {
                return row(idxRowType,
                           ValueSources.toObject(newRow.value(1)),
                           ValueSources.toObject(newRow.value(2)),
                           ValueSources.toObject(newRow.value(3)),
                           ValueSources.toObject(newRow.value(0)));
            }
        }
        fail();
        return null;
    }

    // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int ID = 3;
    private static final boolean ASC = true;
    private static final boolean DESC = false;
    private static final boolean EXCLUSIVE = false;
    private static final boolean INCLUSIVE = true;
    private static final Integer UNSPECIFIED = new Integer(Integer.MIN_VALUE); // Relying on == comparisons

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
}
