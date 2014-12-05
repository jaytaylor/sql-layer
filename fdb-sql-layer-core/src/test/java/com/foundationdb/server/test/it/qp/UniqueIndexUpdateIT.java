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

import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;

import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.*;

public class UniqueIndexUpdateIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "y int",
            "primary key (id)");
        createUniqueIndex("schema", "t", "idx_xy", "x", "y");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        xyIndexRowType = indexType(t, "x", "y");
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    @Test
    public void testNullOnInsert()
    {
        writeRow(t, 1000L, 1L, 1L);
        writeRow(t, 2000L, 2L, 2L);
        writeRow(t, 3000L, 3L, null);
        writeRow(t, 4000L, 4L, null);
        Operator plan = indexScan_Default(xyIndexRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        Row row;
        int count = 0;
        while ((row = cursor.next()) != null) {
            IndexRow indexRow = (IndexRow) row;
            long x = getLong(indexRow, 0);
            long id = getLong(indexRow, 2);
            assertEquals(id, x * 1000);
            switch((int)x) {
                case 1:
                case 2:
                    assertEquals(x, getLong(indexRow, 1).longValue());
                break;
                case 3:
                case 4:
                    assertTrue(isNull(indexRow, 1));
                break;
                default:
                    fail();
            }
            count++;
        }
        assertEquals(4, count);
    }

    @Test
    public void testNullOnUpdate()
    {
        // Load as in testNullSeparatorOnInsert
        writeRow(t, 1000L, 1L, 1L);
        writeRow(t, 2000L, 2L, 2L);
        writeRow(t, 3000L, 3L, null);
        writeRow(t, 4000L, 4L, null);
        // Change nulls to some other value. Scan backwards to avoid halloween issues.
        Cursor cursor = cursor(indexScan_Default(xyIndexRowType, true), queryContext, queryBindings);
        cursor.openTopLevel();
        Row row;
        final long NEW_Y_VALUE = 99;
        while ((row = cursor.next()) != null) {
            IndexRow indexRow = (IndexRow) row;
            long x = getLong(indexRow, 0);
            long id = getLong(indexRow, 2);
            int pos = 1;
            if (isNull(indexRow, pos)) {
                Row oldRow = row(t, id, x, null);
                Row newRow = row(t, id, x, NEW_Y_VALUE);
                updateRow(oldRow, newRow);
            }
        }
        cursor.close();
        // Check final state
        cursor = cursor(indexScan_Default(xyIndexRowType), queryContext, queryBindings);
        cursor.openTopLevel();
        int count = 0;
        while ((row = cursor.next()) != null) {
            IndexRow indexRow = (IndexRow) row;
            long x = getLong(indexRow, 0);
            long y = getLong(indexRow, 1);
            long id = getLong(indexRow, 2);
            assertEquals(id, x * 1000);
            if (id <= 2000) {
                assertEquals(id, y * 1000);
            } else {
                assertEquals(NEW_Y_VALUE, y);
            }
            count++;
        }
        assertEquals(4, count);
    }

    @Test
    public void testDeleteIndexRowWithNull()
    {
        writeRow(t, 1L, 999L, null);
        writeRow(t, 2L, 999L, null);
        writeRow(t, 3L, 999L, null);
        writeRow(t, 4L, 999L, null);
        writeRow(t, 5L, 999L, null);
        writeRow(t, 6L, 999L, null);
        checkIndex(1, 2, 3, 4, 5, 6);
        // Delete each row
        deleteRow(t, 3L, 999L, null);
        checkIndex(1, 2, 4, 5, 6);
        deleteRow(t, 6L, 999L, null);
        checkIndex(1, 2, 4, 5);
        deleteRow(t, 2L, 999L, null);
        checkIndex(1, 4, 5);
        deleteRow(t, 4L, 999L, null);
        checkIndex(1, 5);
        deleteRow(t, 1L, 999L, null);
        checkIndex(5);
        deleteRow(t, 5L, 999L, null);
        checkIndex();
    }

    private void checkIndex(long ... expectedIds)
    {
        Cursor cursor = cursor(indexScan_Default(xyIndexRowType), queryContext, queryBindings);
        cursor.openTopLevel();
        Row row;
        int count = 0;
        while ((row = cursor.next()) != null) {
            long id = getLong(row, 2);
            assertEquals(expectedIds[count], id);
            count++;
        }
        assertEquals(expectedIds.length, count);
    }

    // Inspired by bug 1036389

    @Test
    public void testUpdateIndexRowWithNull()
    {
        db = new Row[]{
            row(t, 1L, null, null),
        };
        use(db);
        Row oldRow = row(t, 1L, null, null);
        Row newRow = row(t, 1L, 10L, 10L);
        updateRow(oldRow, newRow);
        Cursor cursor = cursor(indexScan_Default(xyIndexRowType), queryContext, queryBindings);
        cursor.openTopLevel();
        Row row = cursor.next();
        assertEquals(Long.valueOf(10), getLong(row, 0));
        assertEquals(Long.valueOf(10), getLong(row, 1));
        assertEquals(Long.valueOf(1), getLong(row, 2));
        row = cursor.next();
        assertNull(row);
    }

    private int t;
    private TableRowType tRowType;
    private IndexRowType xyIndexRowType;
}
