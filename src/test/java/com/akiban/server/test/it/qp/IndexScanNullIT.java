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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 * This test covers index scans bound with null on one or both sides
 * lo > null                   lo is unbounded, only valid in last part of IndexKeyRange
 * lo >= null                  lo is unbounded or null, only valid in last part of IndexKeyRange unless hi <= null.
 * hi < null                   hi is unbounded, only valid in last part of IndexKeyRange
 * hi <= null                  invalid
 * lo >= null and hi <= null   IS NULL comparison
 */

public class IndexScanNullIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null key",
            "a int",
            "b int",
            "index(a, b, id)");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "a", "b", "id");
        db = new NewRow[]{
            // No nulls
            createNewRow(t, 1000L, null, null),
            createNewRow(t, 1001L, null, 8L),
            createNewRow(t, 1002L, 5L, null),
            createNewRow(t, 1003L, 5L, 8L),
        };
        adapter = persistitAdapter(schema);
        use(db);
    }

    @Ignore
    @Test
    public void testGTNull()
    {
        test(range(EXCLUSIVE, null, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED),
             ordering(ASC),
             1002, 1003);
        test(range(EXCLUSIVE, null, null,
                   INCLUSIVE, null, 8),
             ordering(ASC, ASC),
             1001);
        test(range(EXCLUSIVE, null, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED),
             ordering(DESC),
             1003, 1002);
        test(range(EXCLUSIVE, null, null,
                   INCLUSIVE, null, 8),
             ordering(DESC, DESC),
             1001);
        test(range(EXCLUSIVE, null, null,
                   INCLUSIVE, null, 8),
             ordering(ASC, DESC),
             1001);
    }

    @Ignore
    @Test
    public void testGENull()
    {
        test(range(INCLUSIVE, null, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, null, null,
                   INCLUSIVE, null, 8),
             ordering(ASC, ASC),
             1000, 1001);
    }

    @Ignore
    @Test
    public void testLTNull()
    {
        test(range(INCLUSIVE, 5, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED),
             ordering(ASC),
             1002, 1003);
        test(range(INCLUSIVE, null, 8,
                   EXCLUSIVE, null, null),
             ordering(ASC, ASC),
             1001);
    }

    @Ignore
    @Test
    public void testLENull()
    {
        // restrict one column, unidirectional
        try {
            test(range(INCLUSIVE, 5, UNSPECIFIED,
                       INCLUSIVE, null, UNSPECIFIED),
                 ordering(ASC));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        // restrict both columns, unidirectional
        try {
            test(range(INCLUSIVE, null, 8,
                       INCLUSIVE, null, null),
                 ordering(ASC, ASC));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        // restrict one column, unidirectional
        try {
            test(range(INCLUSIVE, 5, UNSPECIFIED,
                       INCLUSIVE, null, UNSPECIFIED),
                 ordering(DESC));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        // restrict both columns, unidirectional
        try {
            test(range(INCLUSIVE, null, 8,
                       INCLUSIVE, null, null),
                 ordering(DESC, DESC));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        // restrict both columns, mixed-mode
        try {
            test(range(INCLUSIVE, null, 8,
                       INCLUSIVE, null, null),
                 ordering(ASC, DESC));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Ignore
    @Test
    public void testISNull()
    {
        test(range(INCLUSIVE, null, UNSPECIFIED,
                   INCLUSIVE, null, UNSPECIFIED),
             ordering(ASC),
             1000, 1001);
        test(range(INCLUSIVE, null, null,
                   INCLUSIVE, null, null),
             ordering(ASC, ASC),
             1000);
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
        compareRows(expected, cursor(plan, adapter));
    }

    private void dump(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        dumpToAssertion(plan);
    }

    private IndexKeyRange range(boolean loInclusive, Integer aLo, Integer bLo,
                                boolean hiInclusive, Integer aHi, Integer bHi)
    {
        IndexBound lo;
        if (aLo == UNSPECIFIED) {
            lo = null;
            fail();
        } else if (bLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo), new SetColumnSelector(0));
        } else {
            lo = new IndexBound(row(idxRowType, aLo, bLo), new SetColumnSelector(0, 1));
        }
        IndexBound hi;
        if (aHi == UNSPECIFIED) {
            hi = null;
            fail();
        } else if (bHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi), new SetColumnSelector(0));
        } else {
            hi = new IndexBound(row(idxRowType, aHi, bHi), new SetColumnSelector(0, 1));
        }
        return IndexKeyRange.bounded(idxRowType, lo, loInclusive, hi, hiInclusive);
    }

    private API.Ordering ordering(boolean... directions)
    {
        assertTrue(directions.length >= 1 && directions.length <= 2);
        API.Ordering ordering = API.ordering();
        if (directions.length >= 1) {
            ordering.append(new FieldExpression(idxRowType, A), directions[0]);
        }
        if (directions.length >= 2) {
            ordering.append(new FieldExpression(idxRowType, B), directions[1]);
        }
        return ordering;
    }

    private RowBase dbRow(long id)
    {
        for (NewRow newRow : db) {
            if (newRow.get(0).equals(id)) {
                return row(idxRowType, newRow.get(1), newRow.get(2), newRow.get(0));
            }
        }
        fail();
        return null;
    }

    // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
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
