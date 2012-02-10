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

package com.akiban.server.test.it.qp;

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.store.PersistitStore;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static com.akiban.qp.operator.API.ordering;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 * A *semi-bounded* scan is one in which a start or end value is specified and we go to one end of the index or the
 * other. It differs from a one-side-bounded scan in that there are no equality matches. E.g., if we have an index
 * on (x, y), then the one-side-bounded scan >(10, 5) finds all records such that x = 10 and y > 5. A semi-bounded
 * scan finds all records following (10, 5), and could include rows with x > 10. MySQL does semi-bounded scans.
 */

public class IndexScanLexicographicIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null key",
            "a int",
            "b int",
            "c int",
            "index(a, b, c, id)");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new NewRow[]{
            // No nulls
            createNewRow(t, 1000L, 1L, 11L, 111L),
            createNewRow(t, 1001L, 1L, 11L, 115L),
            createNewRow(t, 1002L, 1L, 15L, 151L),
            createNewRow(t, 1003L, 1L, 15L, 155L),
            createNewRow(t, 1004L, 5L, 51L, 511L),
            createNewRow(t, 1005L, 5L, 51L, 515L),
            createNewRow(t, 1006L, 5L, 55L, 551L),
            createNewRow(t, 1007L, 5L, 55L, 555L),
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
    }

    @Test
    public void test_AscFromBound()
    {
        test(start(INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1004, 1005, 1006, 1007);
        test(start(INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC),
             1002, 1003, 1004, 1005, 1006, 1007);
        test(start(INCLUSIVE, 1, 15, 155),
             ordering(ASC),
             1003, 1004, 1005, 1006, 1007);
        test(start(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1004, 1005, 1006, 1007);
        test(start(EXCLUSIVE, 1, 13, UNSPECIFIED),
             ordering(ASC),
             1002, 1003, 1004, 1005, 1006, 1007);
        test(start(EXCLUSIVE, 1, 15, 153),
             ordering(ASC),
             1003, 1004, 1005, 1006, 1007);
    }

    @Test
    public void test_AscToBound()
    {
        test(end(INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(end(INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003);
        test(end(INCLUSIVE, 5, 55, 551),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006);
        test(end(EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(end(EXCLUSIVE, 1, 18, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003);
        test(end(EXCLUSIVE, 5, 55, 553),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006);
    }

    @Test
    public void test_DescFromBound()
    {
        test(end(INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(end(INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC),
             1003, 1002, 1001, 1000);
        test(end(INCLUSIVE, 1, 15, 151),
             ordering(DESC),
             1002, 1001, 1000);
        test(end(EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(end(EXCLUSIVE, 1, 18, UNSPECIFIED),
             ordering(DESC),
             1003, 1002, 1001, 1000);
        test(end(EXCLUSIVE, 1, 15, 153),
             ordering(DESC),
             1002, 1001, 1000);
    }

    @Test
    public void test_DescToBound()
    {
        test(start(INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004);
        test(start(INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002);
        test(start(INCLUSIVE, 1, 11, 115),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001);
        test(start(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004);
        test(start(EXCLUSIVE, 1, 13, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002);
        test(start(EXCLUSIVE, 1, 11, 113),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001);
    }

    // Inspirted by bug 889736
    @Test
    public void test_LoAndHiBounded()
    {
        test(range(INCLUSIVE, 1, 11, 115,
                   INCLUSIVE, 5, 51, 515),
             ordering(ASC),
             1001, 1002, 1003, 1004, 1005);
    }

    // For use by this class

    private void test(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        RowBase[] expected = new RowBase[expectedIds.length];
        for (int i = 0; i < expectedIds.length; i++) {
            int id = expectedIds[i];
            expected[i] = dbRow(id);
        }
        compareRows(expected, cursor(plan, queryContext));
    }

    private void dump(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        dumpToAssertion(plan);
    }

    private IndexKeyRange start(boolean inclusive, Integer a, Integer b, Integer c)
    {
        IndexBound bound;
        if (a == UNSPECIFIED) {
            bound = null;
            fail();
        } else if (b == UNSPECIFIED) {
            bound = new IndexBound(row(idxRowType, a), new SetColumnSelector(0));
        } else if (c == UNSPECIFIED) {
            bound = new IndexBound(row(idxRowType, a, b), new SetColumnSelector(0, 1));
        } else {
            bound = new IndexBound(row(idxRowType, a, b, c), new SetColumnSelector(0, 1, 2));
        }
        IndexKeyRange indexKeyRange = IndexKeyRange.startingAt(idxRowType, bound, inclusive);
        indexKeyRange.lexicographic(true);
        return indexKeyRange;
    }

    private IndexKeyRange end(boolean inclusive, Integer a, Integer b, Integer c)
    {
        IndexBound bound;
        if (a == UNSPECIFIED) {
            bound = null;
            fail();
        } else if (b == UNSPECIFIED) {
            bound = new IndexBound(row(idxRowType, a), new SetColumnSelector(0));
        } else if (c == UNSPECIFIED) {
            bound = new IndexBound(row(idxRowType, a, b), new SetColumnSelector(0, 1));
        } else {
            bound = new IndexBound(row(idxRowType, a, b, c), new SetColumnSelector(0, 1, 2));
        }
        IndexKeyRange indexKeyRange = IndexKeyRange.endingAt(idxRowType, bound, inclusive);
        indexKeyRange.lexicographic(true);
        return indexKeyRange;
    }

    private IndexKeyRange range(boolean loInclusive, Integer aLo, Integer bLo, Integer cLo,
                                boolean hiInclusive, Integer aHi, Integer bHi, Integer cHi)
    {
        IndexBound lo;
        if (aLo == UNSPECIFIED) {
            lo = null;
            fail();
        } else if (bLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo), new SetColumnSelector(0));
        } else if (cLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo, bLo), new SetColumnSelector(0, 1));
        } else {
            lo = new IndexBound(row(idxRowType, aLo, bLo, cLo), new SetColumnSelector(0, 1, 2));
        }
        IndexBound hi;
        if (aHi == UNSPECIFIED) {
            hi = null;
            fail();
        } else if (bHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi), new SetColumnSelector(0));
        } else if (cHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi, bHi), new SetColumnSelector(0, 1));
        } else {
            hi = new IndexBound(row(idxRowType, aHi, bHi, cHi), new SetColumnSelector(0, 1, 2));
        }
        IndexKeyRange indexKeyRange = IndexKeyRange.bounded(idxRowType, lo, loInclusive, hi, hiInclusive);
        indexKeyRange.lexicographic(true);
        return indexKeyRange;
    }

    private API.Ordering ordering(boolean direction)
    {
        API.Ordering ordering = API.ordering();
        ordering.append(new FieldExpression(idxRowType, 0), direction);
        return ordering;
    }

    private RowBase dbRow(long id)
    {
        for (NewRow newRow : db) {
            if (newRow.get(0).equals(id)) {
                return row(idxRowType, newRow.get(1), newRow.get(2), newRow.get(3), newRow.get(0));
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
