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
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.value.ValueSources;
import org.junit.Test;

import java.util.*;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertEquals;

public class IndexScanJumpBoundedIT extends OperatorITBase
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
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new Row[] {
            row(t, 1010L, 1L, 11L, 111L),
            row(t, 1011L, 1L, 11L, 112L),
            row(t, 1012L, 1L, 12L, 121L),
            row(t, 1013L, 1L, 12L, 122L),
            row(t, 1014L, 1L, 13L, 131L),
            row(t, 1015L, 1L, 13L, 132L),
        };
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
        for (Row row : db) {
            indexRowMap.put(ValueSources.getLong(row.value(0)),
                            new TestRow(tRowType,
                                        ValueSources.toObject(row.value(1)),    // a
                                        ValueSources.toObject(row.value(2)),    // b
                                        ValueSources.toObject(row.value(3)),    // c
                                        ValueSources.toObject(row.value(0))));  // id
        }
    }

    @Test
    public void testAAAA()
    {
        API.Ordering ordering = ordering(A, ASC, B, ASC, C, ASC, ID, ASC);
        long[] idOrdering = longs(1010, 1011, 1012, 1013, 1014, 1015);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), -1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testAAAD()
    {
        API.Ordering ordering = ordering(A, ASC, B, ASC, C, ASC, ID, DESC);
        long[] idOrdering = longs(1010, 1011, 1012, 1013, 1014, 1015);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), 1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testAADA()
    {
        API.Ordering ordering = ordering(A, ASC, B, ASC, C, DESC, ID, ASC);
        long[] idOrdering = longs(1011, 1010, 1013, 1012, 1015, 1014);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), -1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testAADD()
    {
        API.Ordering ordering = ordering(A, ASC, B, ASC, C, DESC, ID, DESC);
        long[] idOrdering = longs(1011, 1010, 1013, 1012, 1015, 1014);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), 1);
            cursor.closeTopLevel();
        }

    }

    @Test
    public void testADAA()
    {
        API.Ordering ordering = ordering(A, ASC, B, DESC, C, ASC, ID, ASC);
        long[] idOrdering = longs(1014, 1015, 1012, 1013, 1010, 1011);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), -1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testADAD()
    {
        API.Ordering ordering = ordering(A, ASC, B, DESC, C, ASC, ID, DESC);
        long[] idOrdering = longs(1014, 1015, 1012, 1013, 1010, 1011);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), 1);
            cursor.closeTopLevel();
        }

    }

    @Test
    public void testADDA()
    {
        API.Ordering ordering = ordering(A, ASC, B, DESC, C, DESC, ID, ASC);
        long[] idOrdering = longs(1015, 1014, 1013, 1012, 1011, 1010);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), -1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testADDD()
    {
        API.Ordering ordering = ordering(A, ASC, B, DESC, C, DESC, ID, DESC);
        long[] idOrdering = longs(1015, 1014, 1013, 1012, 1011, 1010);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), 1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testDAAA()
    {
        API.Ordering ordering = ordering(A, DESC, B, ASC, C, ASC, ID, ASC);
        long[] idOrdering = longs(1010, 1011, 1012, 1013, 1014, 1015);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), -1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testDAAD()
    {
        API.Ordering ordering = ordering(A, DESC, B, ASC, C, ASC, ID, DESC);
        long[] idOrdering = longs(1010, 1011, 1012, 1013, 1014, 1015);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), 1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testDADA()
    {
        API.Ordering ordering = ordering(A, DESC, B, ASC, C, DESC, ID, ASC);
        long[] idOrdering = longs(1011, 1010, 1013, 1012, 1015, 1014);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), -1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testDADD()
    {
        API.Ordering ordering = ordering(A, DESC, B, ASC, C, DESC, ID, DESC);
        long[] idOrdering = longs(1011, 1010, 1013, 1012, 1015, 1014);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), 1);
            cursor.closeTopLevel();
        }

    }

    @Test
    public void testDDAA()
    {
        API.Ordering ordering = ordering(A, DESC, B, DESC, C, ASC, ID, ASC);
        long[] idOrdering = longs(1014, 1015, 1012, 1013, 1010, 1011);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), -1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testDDAD()
    {
        API.Ordering ordering = ordering(A, DESC, B, DESC, C, ASC, ID, DESC);
        long[] idOrdering = longs(1014, 1015, 1012, 1013, 1010, 1011);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), 1);
            cursor.closeTopLevel();
        }

    }
    
    @Test
    public void testDDADLegalRange()
    {
        testRange(getDDAD(),
                  getDDADId(),
                  0,
                  10, true,
                  14, true,
                  new long [][]
                  {
                      {1014, 1015, 1012, 1013, 1010, 1011},
                      {1015, 1012, 1013, 1010, 1011},
                      {1012, 1013, 1010, 1011},
                      {1013, 1010, 1011},
                      {1010, 1011},
                      {1011}

                  });
    }

    @Test
    public void testDDADOutOfRangeUpperBound()
    {
        testRange(getDDAD(),
                  getDDADId(),
                  0,
                  12, true,
                  16, true,
                  new long [][]
                  {
                      {1014, 1015, 1012, 1013},
                      {1015, 1012, 1013},
                      {1012, 1013},
                      {1013},
                      {1014, 1015, 1012, 1013},
                      {1014, 1015, 1012, 1013}
                  });
    }

    @Test
    public void testDDADOutOfRangeLowerBound()
    {
        testRange(getDDAD(),
                  getDDADId(),
                  0,
                  9, true,
                  14, true,
                  new long [][]
                  {
                        {1014, 1015, 1012, 1013, 1010, 1011},
                        {1015, 1012, 1013, 1010, 1011},
                        {1012, 1013, 1010, 1011},
                        {1013, 1010, 1011},
                        {1010, 1011},
                        {1011}
                  });
    }

    @Test
    public void testDDDA()
    {
        API.Ordering ordering = ordering(A, DESC, B, DESC, C, DESC, ID, ASC);
        long[] idOrdering = longs(1015, 1014, 1013, 1012, 1011, 1010);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), -1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), -1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testDDDALegalRange()
    {
        testRange(getDDDA(),
                  getDDDAId(),
                  0,
                  13, true,
                  15, true,
                  new long [][]     // range: [15 - 13]
                  {
                      {1015, 1014},
                      {1014},
                      {},
                      {},
                      {},
                      {}
                  });
    }

     
    @Test
    public void testDDDAOutOfRangeLowerBound()
    {
        testRange(getDDDA(),
                  getDDDAId(),
                  0,
                  1, true,
                  15, true,
                  new long [][]     // specified range [15 - 1]
                  {                 // but it should still only see [15 - 10]
                      {1015, 1014, 1013, 1012, 1011, 1010},
                      {1014, 1013, 1012, 1011, 1010},
                      {1013, 1012, 1011, 1010},
                      {1012, 1011, 1010},
                      {1011, 1010},
                      {1010}
                  });
    }

    @Test
    public void testDDDAOutOfRangeUpperBound()
    {
        testRange(getDDDA(),
                  getDDDAId(),
                  0,
                  12, true,
                  30, true,
                  new long [][]     // specified range [30 - 12]
                  {                 // but it should still only see [15 - 12]
                      {1015, 1014, 1013, 1012},
                      {1014, 1013, 1012},
                      {1013, 1012},
                      {1012},
                      {},
                      {}

                  });
    }

    @Test
    public void testDDDD()
    {
        API.Ordering ordering = ordering(A, DESC, B, DESC, C, DESC, ID, DESC);
        long[] idOrdering = longs(1015, 1014, 1013, 1012, 1011, 1010);
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, idOrdering, 0);
            testJump(cursor, idOrdering, 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, true, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, last4(idOrdering), 0);
            testJump(cursor, last4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, true), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, first4(idOrdering), 0);
            testJump(cursor, first4(idOrdering), 1);
            cursor.closeTopLevel();
        }
        {
            Operator plan = indexScan_Default(idxRowType, bounded(1, 11, false, 13, false), ordering);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            testJump(cursor, middle2(idOrdering), 0);
            testJump(cursor, middle2(idOrdering), 1);
            cursor.closeTopLevel();
        }
    }

    @Test
    public void testDDDDLegalRange()
    {
        testRange(getDDDD(),
                  getDDDDId(),
                  0,
                  13, true,
                  15, true,
                  new long [][]     // range is: [1015 - 1013]
                  {
                     {1015, 1014}, 
                     {1014},
                     {},
                     {},
                     {},
                     {}
                  });
    }

    @Test
    public void testDDDDOutOfRangeUpperBound()
    {   
        testRange(getDDDD(),
                  getDDDDId(),
                  0,
                  12, true,
                  21, true,
                  new long [][]
                  {
                     {1015, 1014, 1013, 1012}, 
                     {1014, 1013, 1012},
                     {1013, 1012},
                     {1012},
                     {},
                     {}
                  });
    }

    @Test
    public void testDDDDOutOfRangeLowerBound()
    {
        testRange(getDDDD(),
                  getDDDDId(),
                  0,
                  3, true,
                  14, true,
                  new long [][]     // specified range: [14-3],
                  {                 // but it should still only see  [10`5 - 1010]
                     {1015, 1014, 1013, 1012, 1011, 1010}, 
                     {1014, 1013, 1012, 1011, 1010},
                     {1013, 1012, 1011, 1010},
                     {1012, 1011, 1010},
                     {1011, 1010},
                     {1010}
                  });
    }

    @Test
    public void testDDDDOutOfRange() // both upper and lower bound
    {
        testRange(getDDDD(),
                  getDDDDId(),
                  0,
                  8, true,
                  20, true,
                  new long [][]     // specified range: [20 - 8],
                  {                 // but it should still only see [15 - 10]
                     {1015, 1014, 1013, 1012, 1011, 1010}, 
                     {1014, 1013, 1012, 1011, 1010},
                     {1013, 1012, 1011, 1010},
                     {1012, 1011, 1010},
                     {1011, 1010},
                     {1010}
                  });
    }

    private API.Ordering getDADD()
    {
        return ordering(A, DESC, B, ASC, C, DESC, ID, DESC);
    }
    
    private long[] getDADDId()
    {
        return longs(1011, 1010, 1013, 1012, 1015, 1014);
    }

    private API.Ordering getDDAA()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID, ASC);
    }
    
    private long[] getDDAAId()
    {
        return longs(1014, 1015, 1012, 1013, 1010, 1011);
    }

    private API.Ordering getDDAD()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID, DESC);
    }

    private long[] getDDADId()
    {
        return longs(1014, 1015, 1012, 1013, 1010, 1011);
    }

    private API.Ordering getDDDA()
    {
         return ordering(A, DESC, B, DESC, C, DESC, ID, ASC);
    }
    
    private long[] getDDDAId()
    {
        return longs(1015, 1014, 1013, 1012, 1011, 1010);
    }

    private API.Ordering getDDDD()
    {
        return ordering(A, DESC, B, DESC, C, DESC, ID, DESC);
    }
    
    private long[] getDDDDId()
    {
        return longs(1015, 1014, 1013, 1012, 1011, 1010);
    }

    private void testRange(API.Ordering ordering,
                           long idOrdering[],
                           int nudge,
                           int lo, boolean loInclusive,
                           int hi, boolean hiInclusive,
                           long expectedArs[][])
    {
        Operator plan = indexScan_Default(idxRowType, bounded(1, lo, loInclusive, hi, hiInclusive), ordering);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor,
                 idOrdering,
                 nudge,
                 expectedArs);
        cursor.closeTopLevel();
    }

    private void testJump(Cursor cursor, long idOrdering[], int nudge, long expectedsAr[][])
    {
        List<List<Long>> expecteds = new ArrayList<>(expectedsAr.length);
        
        for (long expectedAr[] : expectedsAr)
        {
            List<Long> expected = new ArrayList<>();
            for (long val : expectedAr)
                expected.add(val);
            expecteds.add(expected);
        }
       
        doTestJump(cursor, idOrdering, nudge, expecteds);
    }

    private void testJump(Cursor cursor, long idOrdering[], int nudge)
    {
        List<List<Long>> expecteds = new ArrayList<>();
        
        
        for (int idIndex = 0; idIndex < idOrdering.length; ++idIndex)
        {
            List<Long> expected = new ArrayList<>();
            for (int i = idIndex; i < idOrdering.length; i++)
                expected.add(idOrdering[i]);
            expecteds.add(expected);
        }
        
        doTestJump(cursor, idOrdering, nudge, expecteds);
    }

    private void doTestJump(Cursor cursor, long idOrdering[], int nudge, List<List<Long>> expecteds)
    {
        for (int start = 0; start < idOrdering.length; ++start)
        {
            TestRow target = indexRow(idOrdering[start]);
            OverlayingRow nudgedTarget = new OverlayingRow(target);
            //TODO: Not use value.getInt32()
            nudgedTarget.overlay(3, (long)(target.value(3).getInt32() + nudge));
            cursor.jump(nudgedTarget, INDEX_ROW_SELECTOR);
            Row row;
            List<Long> actualIds = new ArrayList<>();
            while ((row = cursor.next()) != null) {
                actualIds.add((long)row.value(3).getInt32());
            }

            assertEquals(expecteds.get(start), actualIds);
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

    private IndexKeyRange bounded(long a, long bLo, boolean loInclusive, long bHi, boolean hiInclusive)
    {
        IndexBound lo = new IndexBound(new TestRow(tRowType, new Object[] {a, bLo, null, null}), new SetColumnSelector(0, 1));
        IndexBound hi = new IndexBound(new TestRow(tRowType, new Object[] {a, bHi, null, null}), new SetColumnSelector(0, 1));
        return IndexKeyRange.bounded(idxRowType, lo, loInclusive, hi, hiInclusive);
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

    private long[] first4(long ... x)
    {
        long[] y = new long[4];
        System.arraycopy(x, 0, y, 0, 4);
        return y;
    }

    private long[] last4(long ... x)
    {
        long[] y = new long[4];
        System.arraycopy(x, 2, y, 0, 4);
        return y;
    }

    private long[] middle2(long ... x)
    {
        long[] y = new long[2];
        System.arraycopy(x, 2, y, 0, 2);
        return y;
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
