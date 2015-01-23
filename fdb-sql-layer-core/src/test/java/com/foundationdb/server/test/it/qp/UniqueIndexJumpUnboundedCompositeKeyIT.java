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

import java.util.Arrays;
import java.lang.Long;

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.server.types.value.ValueSources;

import org.junit.Ignore;
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
 * Test jumping with composite key
 * 
 * E.g., (id1, id2)
 * 
 * And then beyond this, it would be a good idea to declare a multiple-column pk, 
 * e.g. (id1, id2), and to try mixed ordering (e.g. id1 asc, id2 desc) and jumping into those keys.
 */
public class UniqueIndexJumpUnboundedCompositeKeyIT extends OperatorITBase
{
     // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int ID1 = 3;
    private static final int ID2 = 4;

    private static final boolean ASC = true;
    private static final boolean DESC = false;

    private static final SetColumnSelector INDEX_ROW_SELECTOR = new SetColumnSelector(0, 1, 2, 3, 4);

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private Map<Long, TestRow> indexRowWithIdMap = new HashMap<>(); // use for jumping

    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id1 int not null",
            "id2 int not null",
            "a int",
            "b int",
            "c int",
            "PRIMARY KEY (id1, id2)");
        createUniqueIndex("schema", "t", "idx", "a", "b", "c");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "c");
        db = new Row[] {
            row(t, 0L, 1010L, 1L, 11L, 110L),
            row(t, 1L, 1011L, 1L, 13L, 130L),
            row(t, 2L, 1012L, 1L, (Long)null, 132L),
            row(t, 2L, 1013L, 1L, (Long)null, 132L),
            row(t, 3L, 1013L, 1L, (Long)null, 132L),
            row(t, 4L, 1014L, 1L, 13L, 133L),
            row(t, 5L, 1015L, 1L, 13L, 134L),
            row(t, 6L, 1016L, 1L, null, 122L),
            row(t, 7L, 1017L, 1L, 14L, 142L),
//            createNewRow(t, 1018L, 1L, 30L, 201L),
//            createNewRow(t, 1019L, 1L, 30L, null),
//            createNewRow(t, 1020L, 1L, 30L, null),
//            createNewRow(t, 1021L, 1L, 30L, null),
//            createNewRow(t, 1022L, 1L, 30L, 300L),
//            createNewRow(t, 1023L, 1L, 40L, 401L)
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
        for (Row row : db)
        {
            indexRowWithIdMap.put(id(ValueSources.getLong(row.value(0)), ValueSources.getLong(row.value(1))),
                                  new TestRow(tRowType,
                                              ValueSources.toObject(row.value(2)),  // a
                                              ValueSources.toObject(row.value(3)),  // b
                                              ValueSources.toObject(row.value(4)),  // c
                                              ValueSources.toObject(row.value(0)),  // id1
                                              ValueSources.toObject(row.value(1))   // id2
                                  ));
        }
    }

    // ==================== START generated

    // 1

    @Test
    public void testAAAAA_b_6_1016()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(6,1016,
            getAAAAA(),
            new long[][]{
                    {6, 1016},
                    {2, 1012},
                    {2, 1013},
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 2

    @Test
    public void testAAAAA_b_3_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(3,1013,
            getAAAAA(),
            new long[][]{
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 3

    @Test
    public void testAAAAA_b_2_1012()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1012,
            getAAAAA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 4

    @Test
    public void testAAAAA_b_2_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1013,
            getAAAAA(),
            new long[][]{
                    {2, 1013},
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 125
    @Test
    public void testDDDDD_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}]


        testUnbounded(6,1016,
            getDDDDD(),
            new long[][]{
                    {6, 1016}
            });

    }

    // 126
    @Test
    public void testDDDDD_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}]


        testUnbounded(3,1013,
            getDDDDD(),
            new long[][]{
                    {3, 1013},
                    {2, 1013},
                    {2, 1012},
                    {6, 1016}
            });

    }

    // 127
    @Test
    public void testDDDDD_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}]


        testUnbounded(2,1012,
            getDDDDD(),
            new long[][]{
                    {2, 1012},
                    {6, 1016}
            });

    }

    // 128
    @Test
    public void testDDDDD_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}]


        testUnbounded(2,1013,
            getDDDDD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {6, 1016}
            });

    }
 
    // ==================== DONE generated
    
    private void testUnbounded(long targetId1,                  // location to jump to
                                long targetId2,
                                API.Ordering ordering,          
                                long expected[][])
    {
        doTest(targetId1, targetId2, unbounded(), ordering, expected);
    }

     private void doTest(long targetId1,                  // location to jump to
                        long targetId2,
                        IndexKeyRange range,
                        API.Ordering ordering,          
                        long expected[][])
    {
        Operator plan = indexScan_Default(idxRowType, range, ordering);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();


        cursor.jump(indexRowWithId(targetId1, targetId2), INDEX_ROW_SELECTOR);

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
    
    private void checkRows(List<Row> actual, long expected[][])
    {
        List<List<Long>> actualList = new ArrayList<>();
        for (Row row : actual)
            actualList.add(Arrays.asList(getLong(row, ID1),
                                         getLong(row, ID2)));
        
        List<List<Long>> expectedList = new ArrayList<>();
        for (long idPair[] : expected)
            expectedList.add(Arrays.asList(idPair[0], idPair[1]));
        
        assertEquals(expectedList, actualList);
    }

    // --- start generated
    // 1
    private API.Ordering getAAAAA()
    {
        return ordering(A, ASC, B, ASC, C, ASC, ID1, ASC, ID2, ASC);
    }

    // 2
    private API.Ordering getAAAAD()
    {
        return ordering(A, ASC, B, ASC, C, ASC, ID1, ASC, ID2, DESC);
    }

    // 3
    private API.Ordering getAAADA()
    {
        return ordering(A, ASC, B, ASC, C, ASC, ID1, DESC, ID2, ASC);
    }

    // 4
    private API.Ordering getAAADD()
    {
        return ordering(A, ASC, B, ASC, C, ASC, ID1, DESC, ID2, DESC);
    }

    // 5
    private API.Ordering getAADAA()
    {
        return ordering(A, ASC, B, ASC, C, DESC, ID1, ASC, ID2, ASC);
    }

    // 6
    private API.Ordering getAADAD()
    {
        return ordering(A, ASC, B, ASC, C, DESC, ID1, ASC, ID2, DESC);
    }

    // 7
    private API.Ordering getAADDA()
    {
        return ordering(A, ASC, B, ASC, C, DESC, ID1, DESC, ID2, ASC);
    }

    // 8
    private API.Ordering getAADDD()
    {
        return ordering(A, ASC, B, ASC, C, DESC, ID1, DESC, ID2, DESC);
    }

    // 9
    private API.Ordering getADAAA()
    {
        return ordering(A, ASC, B, DESC, C, ASC, ID1, ASC, ID2, ASC);
    }

    // 10
    private API.Ordering getADAAD()
    {
        return ordering(A, ASC, B, DESC, C, ASC, ID1, ASC, ID2, DESC);
    }

    // 11
    private API.Ordering getADADA()
    {
        return ordering(A, ASC, B, DESC, C, ASC, ID1, DESC, ID2, ASC);
    }

    // 12
    private API.Ordering getADADD()
    {
        return ordering(A, ASC, B, DESC, C, ASC, ID1, DESC, ID2, DESC);
    }

    // 13
    private API.Ordering getADDAA()
    {
        return ordering(A, ASC, B, DESC, C, DESC, ID1, ASC, ID2, ASC);
    }

    // 14
    private API.Ordering getADDAD()
    {
        return ordering(A, ASC, B, DESC, C, DESC, ID1, ASC, ID2, DESC);
    }

    // 15
    private API.Ordering getADDDA()
    {
        return ordering(A, ASC, B, DESC, C, DESC, ID1, DESC, ID2, ASC);
    }

    // 16
    private API.Ordering getADDDD()
    {
        return ordering(A, ASC, B, DESC, C, DESC, ID1, DESC, ID2, DESC);
    }

    // 17
    private API.Ordering getDAAAA()
    {
        return ordering(A, DESC, B, ASC, C, ASC, ID1, ASC, ID2, ASC);
    }

    // 18
    private API.Ordering getDAAAD()
    {
        return ordering(A, DESC, B, ASC, C, ASC, ID1, ASC, ID2, DESC);
    }

    // 19
    private API.Ordering getDAADA()
    {
        return ordering(A, DESC, B, ASC, C, ASC, ID1, DESC, ID2, ASC);
    }

    // 20
    private API.Ordering getDAADD()
    {
        return ordering(A, DESC, B, ASC, C, ASC, ID1, DESC, ID2, DESC);
    }

    // 21
    private API.Ordering getDADAA()
    {
        return ordering(A, DESC, B, ASC, C, DESC, ID1, ASC, ID2, ASC);
    }

    // 22
    private API.Ordering getDADAD()
    {
        return ordering(A, DESC, B, ASC, C, DESC, ID1, ASC, ID2, DESC);
    }

    // 23
    private API.Ordering getDADDA()
    {
        return ordering(A, DESC, B, ASC, C, DESC, ID1, DESC, ID2, ASC);
    }

    // 24
    private API.Ordering getDADDD()
    {
        return ordering(A, DESC, B, ASC, C, DESC, ID1, DESC, ID2, DESC);
    }

    // 25
    private API.Ordering getDDAAA()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID1, ASC, ID2, ASC);
    }

    // 26
    private API.Ordering getDDAAD()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID1, ASC, ID2, DESC);
    }

    // 27
    private API.Ordering getDDADA()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID1, DESC, ID2, ASC);
    }

    // 28
    private API.Ordering getDDADD()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID1, DESC, ID2, DESC);
    }

    // 29
    private API.Ordering getDDDAA()
    {
        return ordering(A, DESC, B, DESC, C, DESC, ID1, ASC, ID2, ASC);
    }

    // 30
    private API.Ordering getDDDAD()
    {
        return ordering(A, DESC, B, DESC, C, DESC, ID1, ASC, ID2, DESC);
    }

    // 31
    private API.Ordering getDDDDA()
    {
        return ordering(A, DESC, B, DESC, C, DESC, ID1, DESC, ID2, ASC);
    }

    // 32
    private API.Ordering getDDDDD()
    {
        return ordering(A, DESC, B, DESC, C, DESC, ID1, DESC, ID2, DESC);
    }

     // --- done generated

    private TestRow indexRowWithId(long id1, long id2)
    {
        return indexRowWithIdMap.get(id(id1, id2));
    }

    private long id(long id1, long id2)
    {
        return id2 << 4 | id1;
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
