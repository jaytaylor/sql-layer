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

import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertEquals;

public class IndexScanJumpUnboundedIT extends OperatorITBase
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
        createIndex("schema", "t", "idx", "a", "b", "c", "id");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new NewRow[] {
            createNewRow(t, 1000L, null, null, null),
            createNewRow(t, 1001L, null, null, 5L),
            createNewRow(t, 1002L, null, 4L, null),
            createNewRow(t, 1003L, null, 4L, 5L),
            createNewRow(t, 1010L, 1L, 11L, 111L),
            createNewRow(t, 1011L, 1L, 11L, 112L),
            createNewRow(t, 1012L, 1L, 12L, 121L),
            createNewRow(t, 1013L, 1L, 12L, 122L),
            createNewRow(t, 1020L, 2L, 21L, 211L),
            createNewRow(t, 1021L, 2L, 21L, 212L),
            createNewRow(t, 1022L, 2L, 22L, 221L),
            createNewRow(t, 1023L, 2L, 22L, 222L),
            createNewRow(t, 1030L, 3L, null, null),
            createNewRow(t, 1031L, 3L, null, 5L),
            createNewRow(t, 1032L, 3L, 4L, null),
            createNewRow(t, 1033L, 3L, 4L, 5L),
        };
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
        for (NewRow row : db) {
            indexRowMap.put((Long) row.get(0),
                            new TestRow(tRowType,
                                        new Object[] {row.get(1),     // a
                                                      row.get(2),     // b
                                                      row.get(3),     // c
                                                      row.get(0)}));  // id
        }
    }

    @Test
    public void testAAAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, ASC, ID, ASC));
        long[] idOrdering = longs(1000, 1001, 1002, 1003,
                                  1010, 1011, 1012, 1013,
                                  1020, 1021, 1022, 1023,
                                  1030, 1031, 1032, 1033);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, -1);
        cursor.closeTopLevel();
    }

    @Test
    public void testAAAD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, ASC, ID, DESC));
        long[] idOrdering = longs(1000, 1001, 1002, 1003,
                                  1010, 1011, 1012, 1013,
                                  1020, 1021, 1022, 1023,
                                  1030, 1031, 1032, 1033);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, 1);
        cursor.closeTopLevel();
    }

    @Test
    public void testAADA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, DESC, ID, ASC));
        long[] idOrdering = longs(1001, 1000, 1003, 1002,
                                  1011, 1010, 1013, 1012,
                                  1021, 1020, 1023, 1022,
                                  1031, 1030, 1033, 1032);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, -1);
        cursor.closeTopLevel();
    }

    @Test
    public void testAADD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, DESC, ID, DESC));
        long[] idOrdering = longs(1001, 1000, 1003, 1002,
                                  1011, 1010, 1013, 1012,
                                  1021, 1020, 1023, 1022,
                                  1031, 1030, 1033, 1032);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, 1);
        cursor.closeTopLevel();
    }

    @Test
    public void testADAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, ASC, ID, ASC));
        long[] idOrdering = longs(1002, 1003, 1000, 1001,
                                  1012, 1013, 1010, 1011,
                                  1022, 1023, 1020, 1021,
                                  1032, 1033, 1030, 1031);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, -1);
        cursor.closeTopLevel();
    }

    @Test
    public void testADAD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, ASC, ID, DESC));
        long[] idOrdering = longs(1002, 1003, 1000, 1001,
                                  1012, 1013, 1010, 1011,
                                  1022, 1023, 1020, 1021,
                                  1032, 1033, 1030, 1031);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, 1);
        cursor.closeTopLevel();
    }

    @Test
    public void testADDA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, DESC, ID, ASC));
        long[] idOrdering = longs(1003, 1002, 1001, 1000,
                                  1013, 1012, 1011, 1010,
                                  1023, 1022, 1021, 1020,
                                  1033, 1032, 1031, 1030);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, -1);
        cursor.closeTopLevel();
    }

    @Test
    public void testADDD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, DESC, ID, DESC));
        long[] idOrdering = longs(1003, 1002, 1001, 1000,
                                  1013, 1012, 1011, 1010,
                                  1023, 1022, 1021, 1020,
                                  1033, 1032, 1031, 1030);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, 1);
        cursor.closeTopLevel();
    }

    @Test
    public void testDAAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, ASC, ID, ASC));
        long[] idOrdering = longs(1030, 1031, 1032, 1033,
                                  1020, 1021, 1022, 1023,
                                  1010, 1011, 1012, 1013,
                                  1000, 1001, 1002, 1003);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, -1);
        cursor.closeTopLevel();
    }

    @Test
    public void testDAAD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, ASC, ID, DESC));
        long[] idOrdering = longs(1030, 1031, 1032, 1033,
                                  1020, 1021, 1022, 1023,
                                  1010, 1011, 1012, 1013,
                                  1000, 1001, 1002, 1003);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, 1);
        cursor.closeTopLevel();
    }

    @Test
    public void testDADA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, DESC, ID, ASC));
        long[] idOrdering = longs(1031, 1030, 1033, 1032,
                                  1021, 1020, 1023, 1022,
                                  1011, 1010, 1013, 1012,
                                  1001, 1000, 1003, 1002);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, -1);
        cursor.closeTopLevel();
    }

    @Test
    public void testDADD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, DESC, ID, DESC));
        long[] idOrdering = longs(1031, 1030, 1033, 1032,
                                  1021, 1020, 1023, 1022,
                                  1011, 1010, 1013, 1012,
                                  1001, 1000, 1003, 1002);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, 1);
        cursor.closeTopLevel();
    }

    @Test
    public void testDDAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, ASC, ID, ASC));
        long[] idOrdering = longs(1032, 1033, 1030, 1031,
                                  1022, 1023, 1020, 1021,
                                  1012, 1013, 1010, 1011,
                                  1002, 1003, 1000, 1001);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, -1);
        cursor.closeTopLevel();
    }

    @Test
    public void testDDAD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, ASC, ID, DESC));
        long[] idOrdering = longs(1032, 1033, 1030, 1031,
                                  1022, 1023, 1020, 1021,
                                  1012, 1013, 1010, 1011,
                                  1002, 1003, 1000, 1001);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, 1);
        cursor.closeTopLevel();
    }

    @Test
    public void testDDDA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, DESC, ID, ASC));
        long[] idOrdering = longs(1033, 1032, 1031, 1030,
                                  1023, 1022, 1021, 1020,
                                  1013, 1012, 1011, 1010,
                                  1003, 1002, 1001, 1000);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, -1);
        cursor.closeTopLevel();
    }

    @Test
    public void testDDDD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, DESC, ID, DESC));
        long[] idOrdering = longs(1033, 1032, 1031, 1030,
                                  1023, 1022, 1021, 1020,
                                  1013, 1012, 1011, 1010,
                                  1003, 1002, 1001, 1000);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, 1);
        cursor.closeTopLevel();
    }

    private void testJump(Cursor cursor, long[] idOrdering, int nudge)
    {
        for (int start = 0; start < idOrdering.length; start++) {
            TestRow target = indexRow(idOrdering[start]);
            // Add nudge to last field
            OverlayingRow nudgedTarget = new OverlayingRow(target);
            nudgedTarget.overlay(3, ((long)target.value(3).getInt32() + nudge));
            cursor.jump(nudgedTarget, INDEX_ROW_SELECTOR);
            Row row;
            List<Long> actualIds = new ArrayList<>();
            while ((row = cursor.next()) != null) {
                actualIds.add((long)row.value(3).getInt32());
            }
            List<Long> expectedIds = new ArrayList<>();
            for (int i = start; i < idOrdering.length; i++) {
                expectedIds.add(idOrdering[i]);
            }
            assertEquals(expectedIds, actualIds);
        }
    }

    private TestRow indexRow(long id)
    {
        return indexRowMap.get(id);
    }

    private long[] longs(long... longs)
    {
        return longs;
    }

    private IndexKeyRange unbounded()
    {
        return IndexKeyRange.unbounded(idxRowType);
    }

    private API.Ordering ordering(Object... ord) // alternating column positions and asc/desc
    {
        API.Ordering ordering = API.ordering();
        int i = 0;
        while (i < ord.length) {
            int column = (Integer) ord[i++];
            boolean asc = (Boolean) ord[i++];
            ordering.append(field(idxRowType, column), asc);
        }
        return ordering;
    }

    // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int ID = 3;
    private static final boolean ASC = true;
    private static final boolean DESC = false;
    private static final SetColumnSelector INDEX_ROW_SELECTOR = new SetColumnSelector(0, 1, 2, 3);

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private Map<Long, TestRow> indexRowMap = new HashMap<>();
}
