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
        schema = new Schema(ais());
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
        adapter = newStoreAdapter(schema);
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

    // 5
    @Test
    public void testAAAAD_b_6_1016()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(6,1016,
            getAAAAD(),
            new long[][]{
                    {6, 1016},
                    {2, 1013},
                    {2, 1012},
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 6

    @Test
    public void testAAAAD_b_3_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(3,1013,
            getAAAAD(),
            new long[][]{
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 7
    @Test
    public void testAAAAD_b_2_1012()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1012,
            getAAAAD(),
            new long[][]{
                    {2, 1012},
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 8
    @Test
    public void testAAAAD_b_2_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1013,
            getAAAAD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 9
    @Test
    public void testAAADA_b_6_1016()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(6,1016,
            getAAADA(),
            new long[][]{
                    {6, 1016},
                    {3, 1013},
                    {2, 1012},
                    {2, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 10
    @Test
    public void testAAADA_b_3_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(3,1013,
            getAAADA(),
            new long[][]{
                    {3, 1013},
                    {2, 1012},
                    {2, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 11
    @Test
    public void testAAADA_b_2_1012()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1012,
            getAAADA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 12
    @Test
    public void testAAADA_b_2_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1013,
            getAAADA(),
            new long[][]{
                    {2, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 13

    @Test
    public void testAAADD_b_6_1016()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(6,1016,
            getAAADD(),
            new long[][]{
                    {6, 1016},
                    {3, 1013},
                    {2, 1013},
                    {2, 1012},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 14

    @Test
    public void testAAADD_b_3_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(3,1013,
            getAAADD(),
            new long[][]{
                    {3, 1013},
                    {2, 1013},
                    {2, 1012},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 15
    @Test
    public void testAAADD_b_2_1012()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1012,
            getAAADD(),
            new long[][]{
                    {2, 1012},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 16

    @Test
    public void testAAADD_b_2_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1013,
            getAAADD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 17

    @Test
    public void testAADAA_b_6_1016()
    {
        // 'correct ordering':
        // [{2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(6,1016,
            getAADAA(),
            new long[][]{
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 18

    @Test
    public void testAADAA_b_3_1013()
    {
        // 'correct ordering':
        // [{2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(3,1013,
            getAADAA(),
            new long[][]{
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 19
    @Test
    public void testAADAA_b_2_1012()
    {
        // 'correct ordering':
        // [{2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1012,
            getAADAA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 20
    @Test
    public void testAADAA_b_2_1013()
    {
        // 'correct ordering':
        // [{2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1013,
            getAADAA(),
            new long[][]{
                    {2, 1013},
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 21

    @Test
    public void testAADAD_b_6_1016()
    {
        // 'correct ordering':
        // [{2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(6,1016,
            getAADAD(),
            new long[][]{
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 22

    @Test
    public void testAADAD_b_3_1013()
    {
        // 'correct ordering':
        // [{2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(3,1013,
            getAADAD(),
            new long[][]{
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 23
    @Test
    public void testAADAD_b_2_1012()
    {
        // 'correct ordering':
        // [{2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1012,
            getAADAD(),
            new long[][]{
                    {2, 1012},
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 24
    @Test
    public void testAADAD_b_2_1013()
    {
        // 'correct ordering':
        // [{2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1013,
            getAADAD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 25

    @Test
    public void testAADDA_b_6_1016()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(6,1016,
            getAADDA(),
            new long[][]{
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 26
    @Test
    public void testAADDA_b_3_1013()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(3,1013,
            getAADDA(),
            new long[][]{
                    {3, 1013},
                    {2, 1012},
                    {2, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 27
    @Test
    public void testAADDA_b_2_1012()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1012,
            getAADDA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 28
    @Test
    public void testAADDA_b_2_1013()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1013,
            getAADDA(),
            new long[][]{
                    {2, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 29

    @Test
    public void testAADDD_b_6_1016()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(6,1016,
            getAADDD(),
            new long[][]{
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 30

    @Test
    public void testAADDD_b_3_1013()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(3,1013,
            getAADDD(),
            new long[][]{
                    {3, 1013},
                    {2, 1013},
                    {2, 1012},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 31
    @Test
    public void testAADDD_b_2_1012()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1012,
            getAADDD(),
            new long[][]{
                    {2, 1012},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 32

    @Test
    public void testAADDD_b_2_1013()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1013,
            getAADDD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 33

    @Test
    public void testADAAA_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}]


        testUnbounded(6,1016,
            getADAAA(),
            new long[][]{
                    {6, 1016},
                    {2, 1012},
                    {2, 1013},
                    {3, 1013}
            });

    }

    // 34

    @Test
    public void testADAAA_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}]


        testUnbounded(3,1013,
            getADAAA(),
            new long[][]{
                    {3, 1013}
            });

    }

    // 35
    @Test
    public void testADAAA_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}]


        testUnbounded(2,1012,
            getADAAA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {3, 1013}
            });

    }

    // 36
    @Test
    public void testADAAA_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}]


        testUnbounded(2,1013,
            getADAAA(),
            new long[][]{
                    {2, 1013},
                    {3, 1013}
            });

    }

    // 37
    @Test
    public void testADAAD_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}]


        testUnbounded(6,1016,
            getADAAD(),
            new long[][]{
                    {6, 1016},
                    {2, 1013},
                    {2, 1012},
                    {3, 1013}
            });

    }

    // 38

    @Test
    public void testADAAD_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}]


        testUnbounded(3,1013,
            getADAAD(),
            new long[][]{
                    {3, 1013}
            });

    }

    // 39
    @Test
    public void testADAAD_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}]


        testUnbounded(2,1012,
            getADAAD(),
            new long[][]{
                    {2, 1012},
                    {3, 1013}
            });

    }

    // 40
    @Test
    public void testADAAD_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}]


        testUnbounded(2,1013,
            getADAAD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {3, 1013}
            });

    }

    // 41
    @Test
    public void testADADA_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}]


        testUnbounded(6,1016,
            getADADA(),
            new long[][]{
                    {6, 1016},
                    {3, 1013},
                    {2, 1012},
                    {2, 1013}
            });

    }

    // 42
    @Test
    public void testADADA_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}]


        testUnbounded(3,1013,
            getADADA(),
            new long[][]{
                    {3, 1013},
                    {2, 1012},
                    {2, 1013}
            });

    }

    // 43
    @Test
    public void testADADA_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}]


        testUnbounded(2,1012,
            getADADA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013}
            });

    }

    // 44
    @Test
    public void testADADA_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}]


        testUnbounded(2,1013,
            getADADA(),
            new long[][]{
                    {2, 1013}
            });

    }

    // 45

    @Test
    public void testADADD_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}]


        testUnbounded(6,1016,
            getADADD(),
            new long[][]{
                    {6, 1016},
                    {3, 1013},
                    {2, 1013},
                    {2, 1012}
            });

    }

    // 46

    @Test
    public void testADADD_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}]


        testUnbounded(3,1013,
            getADADD(),
            new long[][]{
                    {3, 1013},
                    {2, 1013},
                    {2, 1012}
            });

    }

    // 47
    @Test
    public void testADADD_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}]


        testUnbounded(2,1012,
            getADADD(),
            new long[][]{
                    {2, 1012}
            });

    }

    // 48

    @Test
    public void testADADD_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}]


        testUnbounded(2,1013,
            getADADD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012}
            });

    }

    // 49

    @Test
    public void testADDAA_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}]


        testUnbounded(6,1016,
            getADDAA(),
            new long[][]{
                    {6, 1016}
            });

    }

    // 50

    @Test
    public void testADDAA_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}]


        testUnbounded(3,1013,
            getADDAA(),
            new long[][]{
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 51
    @Test
    public void testADDAA_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}]


        testUnbounded(2,1012,
            getADDAA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 52
    @Test
    public void testADDAA_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}]


        testUnbounded(2,1013,
            getADDAA(),
            new long[][]{
                    {2, 1013},
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 53

    @Test
    public void testADDAD_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}]


        testUnbounded(6,1016,
            getADDAD(),
            new long[][]{
                    {6, 1016}
            });

    }

    // 54

    @Test
    public void testADDAD_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}]


        testUnbounded(3,1013,
            getADDAD(),
            new long[][]{
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 55
    @Test
    public void testADDAD_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}]


        testUnbounded(2,1012,
            getADDAD(),
            new long[][]{
                    {2, 1012},
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 56
    @Test
    public void testADDAD_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}]


        testUnbounded(2,1013,
            getADDAD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 57

    @Test
    public void testADDDA_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}]


        testUnbounded(6,1016,
            getADDDA(),
            new long[][]{
                    {6, 1016}
            });

    }

    // 58
    @Test
    public void testADDDA_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}]


        testUnbounded(3,1013,
            getADDDA(),
            new long[][]{
                    {3, 1013},
                    {2, 1012},
                    {2, 1013},
                    {6, 1016}
            });

    }

    // 59
    @Test
    public void testADDDA_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}]


        testUnbounded(2,1012,
            getADDDA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {6, 1016}
            });

    }

    // 60
    @Test
    public void testADDDA_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}]


        testUnbounded(2,1013,
            getADDDA(),
            new long[][]{
                    {2, 1013},
                    {6, 1016}
            });

    }

    // 61

    @Test
    public void testADDDD_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}]


        testUnbounded(6,1016,
            getADDDD(),
            new long[][]{
                    {6, 1016}
            });

    }

    // 62

    @Test
    public void testADDDD_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}]


        testUnbounded(3,1013,
            getADDDD(),
            new long[][]{
                    {3, 1013},
                    {2, 1013},
                    {2, 1012},
                    {6, 1016}
            });

    }

    // 63
    @Test
    public void testADDDD_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}]


        testUnbounded(2,1012,
            getADDDD(),
            new long[][]{
                    {2, 1012},
                    {6, 1016}
            });

    }

    // 64

    @Test
    public void testADDDD_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}]


        testUnbounded(2,1013,
            getADDDD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {6, 1016}
            });

    }

    // 65

    @Test
    public void testDAAAA_b_6_1016()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(6,1016,
            getDAAAA(),
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

    // 66

    @Test
    public void testDAAAA_b_3_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(3,1013,
            getDAAAA(),
            new long[][]{
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 67
    @Test
    public void testDAAAA_b_2_1012()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1012,
            getDAAAA(),
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

    // 68
    @Test
    public void testDAAAA_b_2_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1013,
            getDAAAA(),
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

    // 69
    @Test
    public void testDAAAD_b_6_1016()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(6,1016,
            getDAAAD(),
            new long[][]{
                    {6, 1016},
                    {2, 1013},
                    {2, 1012},
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 70

    @Test
    public void testDAAAD_b_3_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(3,1013,
            getDAAAD(),
            new long[][]{
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 71
    @Test
    public void testDAAAD_b_2_1012()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1012,
            getDAAAD(),
            new long[][]{
                    {2, 1012},
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 72
    @Test
    public void testDAAAD_b_2_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1013,
            getDAAAD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {3, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 73
    @Test
    public void testDAADA_b_6_1016()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(6,1016,
            getDAADA(),
            new long[][]{
                    {6, 1016},
                    {3, 1013},
                    {2, 1012},
                    {2, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 74
    @Test
    public void testDAADA_b_3_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(3,1013,
            getDAADA(),
            new long[][]{
                    {3, 1013},
                    {2, 1012},
                    {2, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 75
    @Test
    public void testDAADA_b_2_1012()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1012,
            getDAADA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 76
    @Test
    public void testDAADA_b_2_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1013,
            getDAADA(),
            new long[][]{
                    {2, 1013},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 77

    @Test
    public void testDAADD_b_6_1016()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(6,1016,
            getDAADD(),
            new long[][]{
                    {6, 1016},
                    {3, 1013},
                    {2, 1013},
                    {2, 1012},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 78

    @Test
    public void testDAADD_b_3_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(3,1013,
            getDAADD(),
            new long[][]{
                    {3, 1013},
                    {2, 1013},
                    {2, 1012},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 79
    @Test
    public void testDAADD_b_2_1012()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1012,
            getDAADD(),
            new long[][]{
                    {2, 1012},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 80

    @Test
    public void testDAADD_b_2_1013()
    {
        // 'correct ordering':
        // [{6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}, {0, 1010}, {1, 1011}, {4, 1014}, {5, 1015}, {7, 1017}]


        testUnbounded(2,1013,
            getDAADD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {0, 1010},
                    {1, 1011},
                    {4, 1014},
                    {5, 1015},
                    {7, 1017}
            });

    }

    // 81

    @Test
    public void testDADAA_b_6_1016()
    {
        // 'correct ordering':
        // [{2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(6,1016,
            getDADAA(),
            new long[][]{
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 82

    @Test
    public void testDADAA_b_3_1013()
    {
        // 'correct ordering':
        // [{2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(3,1013,
            getDADAA(),
            new long[][]{
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 83
    @Test
    public void testDADAA_b_2_1012()
    {
        // 'correct ordering':
        // [{2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1012,
            getDADAA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 84
    @Test
    public void testDADAA_b_2_1013()
    {
        // 'correct ordering':
        // [{2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1013,
            getDADAA(),
            new long[][]{
                    {2, 1013},
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 85

    @Test
    public void testDADAD_b_6_1016()
    {
        // 'correct ordering':
        // [{2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(6,1016,
            getDADAD(),
            new long[][]{
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 86

    @Test
    public void testDADAD_b_3_1013()
    {
        // 'correct ordering':
        // [{2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(3,1013,
            getDADAD(),
            new long[][]{
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 87
    @Test
    public void testDADAD_b_2_1012()
    {
        // 'correct ordering':
        // [{2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1012,
            getDADAD(),
            new long[][]{
                    {2, 1012},
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 88
    @Test
    public void testDADAD_b_2_1013()
    {
        // 'correct ordering':
        // [{2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1013,
            getDADAD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {3, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 89

    @Test
    public void testDADDA_b_6_1016()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(6,1016,
            getDADDA(),
            new long[][]{
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 90
    @Test
    public void testDADDA_b_3_1013()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(3,1013,
            getDADDA(),
            new long[][]{
                    {3, 1013},
                    {2, 1012},
                    {2, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 91
    @Test
    public void testDADDA_b_2_1012()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1012,
            getDADDA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 92
    @Test
    public void testDADDA_b_2_1013()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1013,
            getDADDA(),
            new long[][]{
                    {2, 1013},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 93

    @Test
    public void testDADDD_b_6_1016()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(6,1016,
            getDADDD(),
            new long[][]{
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 94

    @Test
    public void testDADDD_b_3_1013()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(3,1013,
            getDADDD(),
            new long[][]{
                    {3, 1013},
                    {2, 1013},
                    {2, 1012},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 95
    @Test
    public void testDADDD_b_2_1012()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1012,
            getDADDD(),
            new long[][]{
                    {2, 1012},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 96

    @Test
    public void testDADDD_b_2_1013()
    {
        // 'correct ordering':
        // [{3, 1013}, {2, 1013}, {2, 1012}, {6, 1016}, {0, 1010}, {5, 1015}, {4, 1014}, {1, 1011}, {7, 1017}]


        testUnbounded(2,1013,
            getDADDD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {6, 1016},
                    {0, 1010},
                    {5, 1015},
                    {4, 1014},
                    {1, 1011},
                    {7, 1017}
            });

    }

    // 97

    @Test
    public void testDDAAA_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}]


        testUnbounded(6,1016,
            getDDAAA(),
            new long[][]{
                    {6, 1016},
                    {2, 1012},
                    {2, 1013},
                    {3, 1013}
            });

    }

    // 98

    @Test
    public void testDDAAA_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}]


        testUnbounded(3,1013,
            getDDAAA(),
            new long[][]{
                    {3, 1013}
            });

    }

    // 99
    @Test
    public void testDDAAA_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}]


        testUnbounded(2,1012,
            getDDAAA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {3, 1013}
            });

    }

    // 100
    @Test
    public void testDDAAA_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1012}, {2, 1013}, {3, 1013}]


        testUnbounded(2,1013,
            getDDAAA(),
            new long[][]{
                    {2, 1013},
                    {3, 1013}
            });

    }

    // 101
    @Test
    public void testDDAAD_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}]


        testUnbounded(6,1016,
            getDDAAD(),
            new long[][]{
                    {6, 1016},
                    {2, 1013},
                    {2, 1012},
                    {3, 1013}
            });

    }

    // 102

    @Test
    public void testDDAAD_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}]


        testUnbounded(3,1013,
            getDDAAD(),
            new long[][]{
                    {3, 1013}
            });

    }

    // 103
    @Test
    public void testDDAAD_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}]


        testUnbounded(2,1012,
            getDDAAD(),
            new long[][]{
                    {2, 1012},
                    {3, 1013}
            });

    }

    // 104
    @Test
    public void testDDAAD_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {2, 1013}, {2, 1012}, {3, 1013}]


        testUnbounded(2,1013,
            getDDAAD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {3, 1013}
            });

    }

    // 105
    @Test
    public void testDDADA_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}]


        testUnbounded(6,1016,
            getDDADA(),
            new long[][]{
                    {6, 1016},
                    {3, 1013},
                    {2, 1012},
                    {2, 1013}
            });

    }

    // 106
    @Test
    public void testDDADA_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}]


        testUnbounded(3,1013,
            getDDADA(),
            new long[][]{
                    {3, 1013},
                    {2, 1012},
                    {2, 1013}
            });

    }

    // 107
    @Test
    public void testDDADA_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}]


        testUnbounded(2,1012,
            getDDADA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013}
            });

    }

    // 108
    @Test
    public void testDDADA_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1012}, {2, 1013}]


        testUnbounded(2,1013,
            getDDADA(),
            new long[][]{
                    {2, 1013}
            });

    }

    // 109

    @Test
    public void testDDADD_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}]


        testUnbounded(6,1016,
            getDDADD(),
            new long[][]{
                    {6, 1016},
                    {3, 1013},
                    {2, 1013},
                    {2, 1012}
            });

    }

    // 110

    @Test
    public void testDDADD_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}]


        testUnbounded(3,1013,
            getDDADD(),
            new long[][]{
                    {3, 1013},
                    {2, 1013},
                    {2, 1012}
            });

    }

    // 111
    @Test
    public void testDDADD_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}]


        testUnbounded(2,1012,
            getDDADD(),
            new long[][]{
                    {2, 1012}
            });

    }

    // 112

    @Test
    public void testDDADD_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {1, 1011}, {4, 1014}, {5, 1015}, {0, 1010}, {6, 1016}, {3, 1013}, {2, 1013}, {2, 1012}]


        testUnbounded(2,1013,
            getDDADD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012}
            });

    }

    // 113

    @Test
    public void testDDDAA_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}]


        testUnbounded(6,1016,
            getDDDAA(),
            new long[][]{
                    {6, 1016}
            });

    }

    // 114

    @Test
    public void testDDDAA_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}]


        testUnbounded(3,1013,
            getDDDAA(),
            new long[][]{
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 115
    @Test
    public void testDDDAA_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}]


        testUnbounded(2,1012,
            getDDDAA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 116
    @Test
    public void testDDDAA_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1012}, {2, 1013}, {3, 1013}, {6, 1016}]


        testUnbounded(2,1013,
            getDDDAA(),
            new long[][]{
                    {2, 1013},
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 117

    @Test
    public void testDDDAD_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}]


        testUnbounded(6,1016,
            getDDDAD(),
            new long[][]{
                    {6, 1016}
            });

    }

    // 118

    @Test
    public void testDDDAD_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}]


        testUnbounded(3,1013,
            getDDDAD(),
            new long[][]{
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 119
    @Test
    public void testDDDAD_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}]


        testUnbounded(2,1012,
            getDDDAD(),
            new long[][]{
                    {2, 1012},
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 120
    @Test
    public void testDDDAD_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {2, 1013}, {2, 1012}, {3, 1013}, {6, 1016}]


        testUnbounded(2,1013,
            getDDDAD(),
            new long[][]{
                    {2, 1013},
                    {2, 1012},
                    {3, 1013},
                    {6, 1016}
            });

    }

    // 121

    @Test
    public void testDDDDA_b_6_1016()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}]


        testUnbounded(6,1016,
            getDDDDA(),
            new long[][]{
                    {6, 1016}
            });

    }

    // 122
    @Test
    public void testDDDDA_b_3_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}]


        testUnbounded(3,1013,
            getDDDDA(),
            new long[][]{
                    {3, 1013},
                    {2, 1012},
                    {2, 1013},
                    {6, 1016}
            });

    }

    // 123
    @Test
    public void testDDDDA_b_2_1012()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}]


        testUnbounded(2,1012,
            getDDDDA(),
            new long[][]{
                    {2, 1012},
                    {2, 1013},
                    {6, 1016}
            });

    }

    // 124
    @Test
    public void testDDDDA_b_2_1013()
    {
        // 'correct ordering':
        // [{7, 1017}, {5, 1015}, {4, 1014}, {1, 1011}, {0, 1010}, {3, 1013}, {2, 1012}, {2, 1013}, {6, 1016}]


        testUnbounded(2,1013,
            getDDDDA(),
            new long[][]{
                    {2, 1013},
                    {6, 1016}
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
