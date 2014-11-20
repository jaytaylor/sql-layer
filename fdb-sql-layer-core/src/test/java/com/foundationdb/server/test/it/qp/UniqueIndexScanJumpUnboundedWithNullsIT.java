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

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.server.types.value.ValueSources;
import org.junit.Test;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.foundationdb.qp.rowtype.Schema;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.*;

public class UniqueIndexScanJumpUnboundedWithNullsIT extends OperatorITBase
{
     // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;

    private static final boolean ASC = true;
    private static final boolean DESC = false;

    private static final SetColumnSelector INDEX_ROW_SELECTOR = new SetColumnSelector(0, 1, 2);

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private Map<Long, TestRow> indexRowMap = new HashMap<>();

    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null primary key",
            "a int",
            "b int",
            "c int");
        createUniqueIndex("schema", "t", "idx", "a", "b", "c");
    }

    @Override
    protected void setupPostCreateSchema() {
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "c");
        db = new Row[] {
            row(t, 1010L, 1L, 11L, 110L),
            row(t, 1011L, 1L, 11L, 111L),
            row(t, 1012L, 1L, (Long)null, 122L),
            row(t, 1013L, 1L, (Long)null, 122L),
            row(t, 1014L, 1L, 13L, 132L),
            row(t, 1015L, 1L, 13L, 133L),
            row(t, 1016L, 1L, null, 122L),
            row(t, 1017L, 1L, 14L, 142L),
            row(t, 1018L, 1L, 20L, 201L),
            row(t, 1019L, 1L, 30L, null),
            row(t, 1020L, 1L, 30L, null),
            row(t, 1021L, 1L, 30L, null),
            row(t, 1022L, 1L, 30L, 300L),
            row(t, 1023L, 1L, 40L, 401L),
            row(t, 1024L, 1L, null, 121L),
            row(t, 1025L, 1L, null, 123L)
        };
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
        for (Row row : db) {
            indexRowMap.put(ValueSources.getLong(row.value(0)),
                            new TestRow(tRowType,
                                        ValueSources.toObject(row.value(1)),   // a
                                        ValueSources.toObject(row.value(2)),   // b
                                        ValueSources.toObject(row.value(3)),   // c
                                        ValueSources.toObject(row.value(0))    // id
                                        ));
        }
    }

    /**
     * 
     * @param id
     * @return the b column of this id (used to make the lower and upper bound.
     *         This is to avoid confusion as to what 'b' values correspond to what id
     */
    private int b_of(long id)
    {
        return (int)indexRow(id).value(1).getInt32();
    }

    @Test
    public void testAAA()
    {
        testSkipNulls(1010,
                      getAAA(),
                      new long[]{1010, 1011, 1014, 1015, 1017, 1018, 1019, 1020, 1021, 1022, 1023}); // skip all rows with b = null
    }

    @Test
    public void testAAAToNull()
    {
        testSkipNulls(1012, // jump to one of the nulls
                      getAAA(),
                      new long[] {1012, 1013, 1016, 1025, 1010, 1011, 1014, 1015, 1017, 1018, 1019, 1020, 1021, 1022, 1023}); // should see the nulls first, because null < everything
    }

    @Test
    public void testDDD()
    {
        testSkipNulls(1015,
                      getDDD(),
                      new long[] {1015, 1014, 1011, 1010, 1025, 1016, 1013, 1012, 1024}); // should see the nulls last because null < everything
        
    }

    @Test
    public void testDDDToMinNull()
    {
        testSkipNulls(1024,
                       getDDD(),
                       new long[] {1024});
    }

    @Test
    public void testDDDToMediumNull()
    {
        testSkipNulls(1013,
                      getDDD(),
                      new long[] {1016, 1013, 1012, 1024});
    }

    @Test
    public void testDDDToMaxNull()
    {
        testSkipNulls(1025,
                      getDDD(),
                      new long[] {1025, 1016, 1013, 1012, 1024}); 
    }

    // all the next three tests should return the same thing
    // because 109, 1020 and 1021 all 'mapped' to the identical index row (with null)
    @Test
    public void testDDDToFirstNull()
    {
        testSkipNulls(1019, // jump to the first null
                      getDDD(),
                      new long[] {1021, 1020, 1019, 1018, 1017, 1015, 1014, 1011, 1010, 1025, 1016, 1013, 1012, 1024});
    }

    @Test
    public void testDDDToMiddleNull()
    {
        testSkipNulls(1020, // jump to the middle null
                      getDDD(),
                      new long[] {1021, 1020, 1019, 1018, 1017, 1015, 1014, 1011, 1010, 1025, 1016, 1013, 1012, 1024});
    }

    @Test
    public void testDDDToLastNull()
    {
        testSkipNulls(1021, // jump to the last null
                      getDDD(),
                      new long[] {1021, 1020, 1019, 1018, 1017, 1015, 1014, 1011, 1010, 1025, 1016, 1013, 1012, 1024});
    }

    @Test
    public void testAAD()
    {
        testSkipNulls(1014,
                      getAAD(),
                      new long[] {1014, 1017, 1018, 1022, 1019, 1020, 1021, 1023}); // skips 1016, which is a null
    }
    
    @Test
    public void testAAAToFirstNull()
    {
        testSkipNulls(1019, // jump to the first null
                      getAAA(),
                      new long[] {1019, 1020, 1021, 1022, 1023});
    }

    //TODO: add more test****() if needed

    private void testSkipNulls(long targetId,                  // location to jump to
                               API.Ordering ordering,          
                               long expected[])
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        cursor.jump(indexRow(targetId), INDEX_ROW_SELECTOR);

        Row row;
        List<Row> actualRows = new ArrayList<>();
        while ((row = cursor.next()) != null)
        {
            actualRows.add(row);
        }
        cursor.closeTopLevel();

        // check the list of rows
        checkRows(actualRows, expected);
    }

    private void checkRows(List<Row> actual, long expected[])
    {
        List<Long> actualList = toListOfLong(actual);
        List<Long> expectedList = new ArrayList<>(expected.length);
        for (long val : expected)
            expectedList.add(val);

        assertEquals(expectedList, actualList);
    }

    private List<Long> toListOfLong(List<Row> rows)
    {
        List<Long> ret = new ArrayList<>(rows.size());

        for (Row row : rows)
            ret.add(getLong(row, 3));

        return ret;
    }

    private API.Ordering getAAA()
    {
        return ordering(A, ASC, B, ASC, C, ASC);
    }

    private API.Ordering getAAD()
    {
        return ordering(A, ASC, B, ASC, C, DESC);
    }
    
    private API.Ordering getADA()
    {
        return ordering(A, ASC, B, DESC, C, ASC);
    }

    private API.Ordering getDAA()
    {
        return ordering(A, DESC, B, ASC, C, ASC);
    }

    private API.Ordering getDAD()
    {
        return ordering(A, DESC, B, ASC, C, DESC);
    }

    private API.Ordering getDDA()
    {
        return ordering(A, DESC, B, DESC, C, ASC);
    }


    private API.Ordering getADD()
    {
         return ordering(A, ASC, B, ASC, C, DESC);
    }

    private API.Ordering getDDD()
    {
        return ordering(A, DESC, B, DESC, C, DESC);
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
        while (i < ord.length)
        {
            int column = (Integer) ord[i++];
            boolean asc = (Boolean) ord[i++];
            ordering.append(field(idxRowType, column), asc);
        }
        return ordering;
    }
}
