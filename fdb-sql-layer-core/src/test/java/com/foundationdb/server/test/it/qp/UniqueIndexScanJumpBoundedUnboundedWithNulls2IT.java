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

import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.qp.expression.IndexBound;
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

/**
 * 
 * This differs from UniqueIndexScanJumpBoundedWithNullsIT in that each index row
 * in this test the target row (of the jump) looks like this:  [ a, b, c | id ]
 * , while in the other one, it's [ a, b, c]
 * 
 * (Open to suggestion on a better name)
 */
public class UniqueIndexScanJumpBoundedUnboundedWithNulls2IT extends OperatorITBase
{
     // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int ID = 3;
    
    private static final int INDEX_COLUMN_COUNT = 4;

    private static final boolean ASC = true;
    private static final boolean DESC = false;

    private static final SetColumnSelector INDEX_ROW_SELECTOR = new SetColumnSelector(0, 1, 2, 3);

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private Map<Long, TestRow> indexRowWithIdMap = new HashMap<>(); // use for jumping

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
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "c");
        db = new Row[] {
            row(t, 1010L, 1L, 11L, 110L),
            row(t, 1011L, 1L, 13L, 130L),
            row(t, 1012L, 1L, (Long)null, 132L),
            row(t, 1013L, 1L, (Long)null, 132L),
            row(t, 1014L, 1L, 13L, 133L),
            row(t, 1015L, 1L, 13L, 134L),
            row(t, 1016L, 1L, null, 122L),
            row(t, 1017L, 1L, 14L, 142L),
            row(t, 1018L, 1L, 30L, 201L),
            row(t, 1019L, 1L, 30L, null),
            row(t, 1020L, 1L, 30L, null),
            row(t, 1021L, 1L, 30L, null),
            row(t, 1022L, 1L, 30L, 300L),
            row(t, 1023L, 1L, 40L, 401L)
        };
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
        for (Row row : db)
        {
            indexRowWithIdMap.put(ValueSources.getLong(row.value(0)),
                                  new TestRow(tRowType,
                                              ValueSources.toObject(row.value(1)),  // a
                                              ValueSources.toObject(row.value(2)),  // b
                                              ValueSources.toObject(row.value(3)),  // c
                                              ValueSources.toObject(row.value(0))   // id
                                  ));
        }
    }

    /**
     * 
     * @param id
     * @return the b column of this id (used to make the lower and upper bound.
     *         This is to avoid confusion as to what 'b' values correspond to what id
     */
    private Integer b_of(long id)
    {
        ValueSource val = indexRowWithIdMap.get(id).value(1);
        return (val.isNull() ? null : val.getInt32());
    }

    // test jumping to rows whose c == null
  
        //--- Start generated
        // 1
        @Test
        public void testAAAA_c_1019()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getAAAA(),
                new long[]{1019, 1020, 1021, 1018, 1022});

        }

        // 2
        @Test
        public void testAAAA_c_1020()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getAAAA(),
                new long[]{1020, 1021, 1018, 1022});

        }

        // 3
        @Test
        public void testAAAA_c_1021()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getAAAA(),
                new long[]{1021, 1018, 1022});

        }

        // 4
        @Test
        public void testAAAD_c_1019()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getAAAD(),
                new long[]{1019, 1018, 1022});

        }

        // 5
        @Test
        public void testAAAD_c_1020()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getAAAD(),
                new long[]{1020, 1019, 1018, 1022});

        }

        // 6
        @Test
        public void testAAAD_c_1021()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getAAAD(),
                new long[]{1021, 1020, 1019, 1018, 1022});

        }

        // 7
        @Test
        public void testAADA_c_1019()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getAADA(),
                new long[]{1019, 1020, 1021});

        }

        // 8
        @Test
        public void testAADA_c_1020()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getAADA(),
                new long[]{1020, 1021});

        }

        // 9
        @Test
        public void testAADA_c_1021()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getAADA(),
                new long[]{1021});

        }

        // 10
        @Test
        public void testAADD_c_1019()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getAADD(),
                new long[]{1019});

        }

        // 11
        @Test
        public void testAADD_c_1020()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getAADD(),
                new long[]{1020, 1019});

        }

        // 12
        @Test
        public void testAADD_c_1021()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getAADD(),
                new long[]{1021, 1020, 1019});

        }

        // 13
        @Test
        public void testADAA_c_1019()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getADAA(),
                new long[]{1019, 1020, 1021, 1018, 1022});

        }

        // 14
        @Test
        public void testADAA_c_1020()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getADAA(),
                new long[]{1020, 1021, 1018, 1022});

        }

        // 15
        @Test
        public void testADAA_c_1021()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getADAA(),
                new long[]{1021, 1018, 1022});

        }

        // 16
        @Test
        public void testADAD_c_1019()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getADAD(),
                new long[]{1019, 1018, 1022});

        }

        // 17
        @Test
        public void testADAD_c_1020()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getADAD(),
                new long[]{1020, 1019, 1018, 1022});

        }

        // 18
        @Test
        public void testADAD_c_1021()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getADAD(),
                new long[]{1021, 1020, 1019, 1018, 1022});

        }

        // 19
        @Test
        public void testADDA_c_1019()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getADDA(),
                new long[]{1019, 1020, 1021});

        }

        // 20
        @Test
        public void testADDA_c_1020()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getADDA(),
                new long[]{1020, 1021});

        }

        // 21
        @Test
        public void testADDA_c_1021()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getADDA(),
                new long[]{1021});

        }

        // 22
        @Test
        public void testADDD_c_1019()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getADDD(),
                new long[]{1019});

        }

        // 23
        @Test
        public void testADDD_c_1020()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getADDD(),
                new long[]{1020, 1019});

        }

        // 24
        @Test
        public void testADDD_c_1021()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getADDD(),
                new long[]{1021, 1020, 1019});

        }

        // 25
        @Test
        public void testDAAA_c_1019()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getDAAA(),
                new long[]{1019, 1020, 1021, 1018, 1022});

        }

        // 26
        @Test
        public void testDAAA_c_1020()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getDAAA(),
                new long[]{1020, 1021, 1018, 1022});

        }

        // 27
        @Test
        public void testDAAA_c_1021()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getDAAA(),
                new long[]{1021, 1018, 1022});

        }

        // 28
        @Test
        public void testDAAD_c_1019()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getDAAD(),
                new long[]{1019, 1018, 1022});

        }

        // 29
        @Test
        public void testDAAD_c_1020()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getDAAD(),
                new long[]{1020, 1019, 1018, 1022});

        }

        // 30
        @Test
        public void testDAAD_c_1021()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getDAAD(),
                new long[]{1021, 1020, 1019, 1018, 1022});

        }

        // 31
        @Test
        public void testDADA_c_1019()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getDADA(),
                new long[]{1019, 1020, 1021});

        }

        // 32
        @Test
        public void testDADA_c_1020()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getDADA(),
                new long[]{1020, 1021});

        }

        // 33
        @Test
        public void testDADA_c_1021()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getDADA(),
                new long[]{1021});

        }

        // 34
        @Test
        public void testDADD_c_1019()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getDADD(),
                new long[]{1019});

        }

        // 35
        @Test
        public void testDADD_c_1020()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getDADD(),
                new long[]{1020, 1019});

        }

        // 36
        @Test
        public void testDADD_c_1021()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getDADD(),
                new long[]{1021, 1020, 1019});

        }

        // 37
        @Test
        public void testDDAA_c_1019()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getDDAA(),
                new long[]{1019, 1020, 1021, 1018, 1022});

        }

        // 38
        @Test
        public void testDDAA_c_1020()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getDDAA(),
                new long[]{1020, 1021, 1018, 1022});

        }

        // 39
        @Test
        public void testDDAA_c_1021()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getDDAA(),
                new long[]{1021, 1018, 1022});

        }

        // 40
        @Test
        public void testDDAD_c_1019()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getDDAD(),
                new long[]{1019, 1018, 1022});

        }

        // 41
        @Test
        public void testDDAD_c_1020()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getDDAD(),
                new long[]{1020, 1019, 1018, 1022});

        }

        // 42
        @Test
        public void testDDAD_c_1021()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getDDAD(),
                new long[]{1021, 1020, 1019, 1018, 1022});

        }

        // 43
        @Test
        public void testDDDA_c_1019()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getDDDA(),
                new long[]{1019, 1020, 1021});

        }

        // 44
        @Test
        public void testDDDA_c_1020()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getDDDA(),
                new long[]{1020, 1021});

        }

        // 45
        @Test
        public void testDDDA_c_1021()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getDDDA(),
                new long[]{1021});

        }

        // 46
        @Test
        public void testDDDD_c_1019()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testBounded(1019,
                b_of(1018), true,
                b_of(1021), true,
                getDDDD(),
                new long[]{1019});

        }

        // 47
        @Test
        public void testDDDD_c_1020()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testBounded(1020,
                b_of(1018), true,
                b_of(1021), true,
                getDDDD(),
                new long[]{1020, 1019});

        }

        // 48
        @Test
        public void testDDDD_c_1021()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testBounded(1021,
                b_of(1018), true,
                b_of(1021), true,
                getDDDD(),
                new long[]{1021, 1020, 1019});

        }

    
    // test jumpting to rows  whose b == null
    // There 3 rows with b == null, and 16 cases (2 ^4)
    // Thus there should be 3 * 16 = 48 cases here
    //
    // (The number at the end of the test method's name is the id of the target row)
    
        //--- Start generated
        // 1
        @Test
        public void testAAAA_b_1012()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testUnbounded(1012,
                getAAAA(),
                new long[]{1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023});

        }

        // 2
        @Test
        public void testAAAA_b_1013()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testUnbounded(1013,
                getAAAA(),
                new long[]{1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023});

        }

        // 3
        @Test
        public void testAAAA_b_1016()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testUnbounded(1016,
                getAAAA(),
                new long[]{1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023});

        }

        // 4
        @Test
        public void testDAAA_b_1012()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testUnbounded(1012,
                getDAAA(),
                new long[]{1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023});

        }

        // 5
        @Test
        public void testDAAA_b_1013()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testUnbounded(1013,
                getDAAA(),
                new long[]{1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023});

        }

        // 6
        @Test
        public void testDAAA_b_1016()
        {
            // 'correct ordering':
            // [1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023]


            testUnbounded(1016,
                getDAAA(),
                new long[]{1016, 1012, 1013, 1010, 1011, 1014, 1015, 1017, 1019, 1020, 1021, 1018, 1022, 1023});

        }

        // 7
        @Test
        public void testAAAD_b_1012()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testUnbounded(1012,
                getAAAD(),
                new long[]{1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023});

        }

        // 8
        @Test
        public void testAAAD_b_1013()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testUnbounded(1013,
                getAAAD(),
                new long[]{1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023});

        }

        // 9
        @Test
        public void testAAAD_b_1016()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testUnbounded(1016,
                getAAAD(),
                new long[]{1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023});

        }

        // 10
        @Test
        public void testADAA_b_1012()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testUnbounded(1012,
                getADAA(),
                new long[]{1012, 1013});

        }

        // 11
        @Test
        public void testADAA_b_1013()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testUnbounded(1013,
                getADAA(),
                new long[]{1013});

        }

        // 12
        @Test
        public void testADAA_b_1016()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testUnbounded(1016,
                getADAA(),
                new long[]{1016, 1012, 1013});

        }

        // 13
        @Test
        public void testDADA_b_1012()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testUnbounded(1012,
                getDADA(),
                new long[]{1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023});

        }

        // 14
        @Test
        public void testDADA_b_1013()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testUnbounded(1013,
                getDADA(),
                new long[]{1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023});

        }

        // 15
        @Test
        public void testDADA_b_1016()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testUnbounded(1016,
                getDADA(),
                new long[]{1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023});

        }

        // 16
        @Test
        public void testDDAA_b_1012()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testUnbounded(1012,
                getDDAA(),
                new long[]{1012, 1013});

        }

        // 17
        @Test
        public void testDDAA_b_1013()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testUnbounded(1013,
                getDDAA(),
                new long[]{1013});

        }

        // 18
        @Test
        public void testDDAA_b_1016()
        {
            // 'correct ordering':
            // [1023, 1019, 1020, 1021, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1012, 1013]


            testUnbounded(1016,
                getDDAA(),
                new long[]{1016, 1012, 1013});

        }

        // 19
        @Test
        public void testDDAD_b_1012()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testUnbounded(1012,
                getDDAD(),
                new long[]{1012});

        }

        // 20
        @Test
        public void testDDAD_b_1013()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testUnbounded(1013,
                getDDAD(),
                new long[]{1013, 1012});

        }

        // 21
        @Test
        public void testDDAD_b_1016()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testUnbounded(1016,
                getDDAD(),
                new long[]{1016, 1013, 1012});

        }

        // 22
        @Test
        public void testADAD_b_1012()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testUnbounded(1012,
                getADAD(),
                new long[]{1012});

        }

        // 23
        @Test
        public void testADAD_b_1013()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testUnbounded(1013,
                getADAD(),
                new long[]{1013, 1012});

        }

        // 24
        @Test
        public void testADAD_b_1016()
        {
            // 'correct ordering':
            // [1023, 1021, 1020, 1019, 1018, 1022, 1017, 1011, 1014, 1015, 1010, 1016, 1013, 1012]


            testUnbounded(1016,
                getADAD(),
                new long[]{1016, 1013, 1012});

        }

        // 25
        @Test
        public void testADDA_b_1012()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testUnbounded(1012,
                getADDA(),
                new long[]{1012, 1013, 1016});

        }

        // 26
        @Test
        public void testADDA_b_1013()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testUnbounded(1013,
                getADDA(),
                new long[]{1013, 1016});

        }

        // 27
        @Test
        public void testADDA_b_1016()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testUnbounded(1016,
                getADDA(),
                new long[]{1016});

        }

        // 28
        @Test
        public void testDADD_b_1012()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testUnbounded(1012,
                getDADD(),
                new long[]{1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023});

        }

        // 29
        @Test
        public void testDADD_b_1013()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testUnbounded(1013,
                getDADD(),
                new long[]{1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023});

        }

        // 30
        @Test
        public void testDADD_b_1016()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testUnbounded(1016,
                getDADD(),
                new long[]{1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023});

        }

        // 31
        @Test
        public void testAADD_b_1012()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testUnbounded(1012,
                getAADD(),
                new long[]{1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023});

        }

        // 32
        @Test
        public void testAADD_b_1013()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testUnbounded(1013,
                getAADD(),
                new long[]{1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023});

        }

        // 33
        @Test
        public void testAADD_b_1016()
        {
            // 'correct ordering':
            // [1013, 1012, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023]


            testUnbounded(1016,
                getAADD(),
                new long[]{1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1021, 1020, 1019, 1023});

        }

        // 34
        @Test
        public void testADDD_b_1012()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testUnbounded(1012,
                getADDD(),
                new long[]{1012, 1016});

        }

        // 35
        @Test
        public void testADDD_b_1013()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testUnbounded(1013,
                getADDD(),
                new long[]{1013, 1012, 1016});

        }

        // 36
        @Test
        public void testADDD_b_1016()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testUnbounded(1016,
                getADDD(),
                new long[]{1016});

        }

        // 37
        @Test
        public void testDDDD_b_1012()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testUnbounded(1012,
                getDDDD(),
                new long[]{1012, 1016});

        }

        // 38
        @Test
        public void testDDDD_b_1013()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testUnbounded(1013,
                getDDDD(),
                new long[]{1013, 1012, 1016});

        }

        // 39
        @Test
        public void testDDDD_b_1016()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1021, 1020, 1019, 1017, 1015, 1014, 1011, 1010, 1013, 1012, 1016]


            testUnbounded(1016,
                getDDDD(),
                new long[]{1016});

        }

        // 40
        @Test
        public void testDDDA_b_1012()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testUnbounded(1012,
                getDDDA(),
                new long[]{1012, 1013, 1016});

        }

        // 41
        @Test
        public void testDDDA_b_1013()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testUnbounded(1013,
                getDDDA(),
                new long[]{1013, 1016});

        }

        // 42
        @Test
        public void testDDDA_b_1016()
        {
            // 'correct ordering':
            // [1023, 1022, 1018, 1019, 1020, 1021, 1017, 1015, 1014, 1011, 1010, 1012, 1013, 1016]


            testUnbounded(1016,
                getDDDA(),
                new long[]{1016});

        }

        // 43
        @Test
        public void testAADA_b_1012()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testUnbounded(1012,
                getAADA(),
                new long[]{1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023});

        }

        // 44
        @Test
        public void testAADA_b_1013()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testUnbounded(1013,
                getAADA(),
                new long[]{1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023});

        }

        // 45
        @Test
        public void testAADA_b_1016()
        {
            // 'correct ordering':
            // [1012, 1013, 1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023]


            testUnbounded(1016,
                getAADA(),
                new long[]{1016, 1010, 1015, 1014, 1011, 1017, 1022, 1018, 1019, 1020, 1021, 1023});

        }

        // 46
        @Test
        public void testDAAD_b_1012()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testUnbounded(1012,
                getDAAD(),
                new long[]{1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023});

        }

        // 47
        @Test
        public void testDAAD_b_1013()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testUnbounded(1013,
                getDAAD(),
                new long[]{1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023});

        }

        // 48
        @Test
        public void testDAAD_b_1016()
        {
            // 'correct ordering':
            // [1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023]


            testUnbounded(1016,
                getDAAD(),
                new long[]{1016, 1013, 1012, 1010, 1011, 1014, 1015, 1017, 1021, 1020, 1019, 1018, 1022, 1023});

        }


        //---- DONE generated

    private void testUnbounded(long targetId,                  // location to jump to
                               API.Ordering ordering,          
                               long expected[])
    {
        doTest(targetId, unbounded(), ordering, expected);
    }

     private void testBounded(long targetId,                  // location to jump to
                               int bLo, boolean lowInclusive,  // lower bound
                               int bHi, boolean hiInclusive,   // upper bound
                               API.Ordering ordering,          
                               long expected[])
    {
        doTest(targetId, bounded(1, bLo, lowInclusive, bHi, hiInclusive), ordering, expected);
    }

    private void doTest(long targetId,                  // location to jump to
                       IndexKeyRange range,
                       API.Ordering ordering,          
                       long expected[])
    {
        Operator plan = indexScan_Default(idxRowType, range, ordering);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();

        cursor.jump(indexRowWithId(targetId), INDEX_ROW_SELECTOR);

        Row row;
        List<Row> actualRows = new ArrayList<>();
        
        while ((row = cursor.next()) != null)
        {
            actualRows.add(row);
        }
        cursor.closeTopLevel();

        // find the row with given id
        List<Row> expectedRows = new ArrayList<>(expected.length);
        for (long val : expected)
            expectedRows.add(indexRowWithId(val));

        // check the list of rows
        checkRows(expectedRows, actualRows);
    }

    private void checkRows(List<Row> expected, List<Row> actual)
    {
        List<List<Long>> expectedRows = toListOfLong(expected);
        List<List<Long>> actualRows = toListOfLong(actual);
        
        assertEquals(expectedRows, actualRows);
    }

    private List<List<Long>> toListOfLong(List<Row> rows)
    {
        List<List<Long>> ret = new ArrayList<>();
        for (Row row : rows)
        {
            // nulls are allowed
            ArrayList<Long> toLong = new ArrayList<>();
            for (int n = 0; n < INDEX_COLUMN_COUNT; ++n) {
                toLong.add(getLong(row, n));
            }
            
            ret.add(toLong);
        }
        return ret;
    }

     // --- start generated
     // 1
    private API.Ordering getAAAA()
    {
        return ordering(A, ASC, B, ASC, C, ASC, ID, ASC);
    }

    // 2
    private API.Ordering getAAAD()
    {
        return ordering(A, ASC, B, ASC, C, ASC, ID, DESC);
    }

    // 3
    private API.Ordering getAADA()
    {
        return ordering(A, ASC, B, ASC, C, DESC, ID, ASC);
    }

    // 4
    private API.Ordering getAADD()
    {
        return ordering(A, ASC, B, ASC, C, DESC, ID, DESC);
    }

    // 5
    private API.Ordering getADAA()
    {
        return ordering(A, ASC, B, DESC, C, ASC, ID, ASC);
    }

    // 6
    private API.Ordering getADAD()
    {
        return ordering(A, ASC, B, DESC, C, ASC, ID, DESC);
    }

    // 7
    private API.Ordering getADDA()
    {
        return ordering(A, ASC, B, DESC, C, DESC, ID, ASC);
    }

    // 8
    private API.Ordering getADDD()
    {
        return ordering(A, ASC, B, DESC, C, DESC, ID, DESC);
    }

    // 9
    private API.Ordering getDAAA()
    {
        return ordering(A, DESC, B, ASC, C, ASC, ID, ASC);
    }

    // 10
    private API.Ordering getDAAD()
    {
        return ordering(A, DESC, B, ASC, C, ASC, ID, DESC);
    }

    // 11
    private API.Ordering getDADA()
    {
        return ordering(A, DESC, B, ASC, C, DESC, ID, ASC);
    }

    // 12
    private API.Ordering getDADD()
    {
        return ordering(A, DESC, B, ASC, C, DESC, ID, DESC);
    }

    // 13
    private API.Ordering getDDAA()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID, ASC);
    }

    // 14
    private API.Ordering getDDAD()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID, DESC);
    }

    // 15
    private API.Ordering getDDDA()
    {
        return ordering(A, DESC, B, DESC, C, DESC, ID, ASC);
    }

    // 16
    private API.Ordering getDDDD()
    {
        return ordering(A, DESC, B, DESC, C, DESC, ID, DESC);
    }

     // --- done generated
    private TestRow indexRowWithId(long id)
    {
        return indexRowWithIdMap.get(id);
    }

    private IndexKeyRange bounded(long a, long bLo, boolean loInclusive, long bHi, boolean hiInclusive)
    {
        IndexBound lo = new IndexBound(new TestRow(tRowType, new Object[] {a, bLo, null, null}), new SetColumnSelector(0, 1));
        IndexBound hi = new IndexBound(new TestRow(tRowType, new Object[] {a, bHi, null, null}), new SetColumnSelector(0, 1));
        return IndexKeyRange.bounded(idxRowType, lo, loInclusive, hi, hiInclusive);
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
