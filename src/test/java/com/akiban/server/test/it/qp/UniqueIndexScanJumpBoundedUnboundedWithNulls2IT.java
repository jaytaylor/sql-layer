/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.qp;

import org.junit.Ignore;
import com.akiban.util.ShareHolder;
import com.akiban.server.types.ValueSource;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.operator.Operator;
import org.junit.Test;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.qp.operator.API;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import com.akiban.qp.rowtype.Schema;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
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
    private Map<Long, TestRow> indexRowMap = new HashMap<Long, TestRow>();
    private Map<Long, TestRow> indexRowWithIdMap = new HashMap<Long, TestRow>(); // use for jumping

    @Before
    @Override
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null primary key",
            "a int",
            "b int",
            "c int");
        createUniqueIndex("schema", "t", "idx", "a", "b", "c");
        schema = new Schema(ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "a", "b", "c");
        db = new NewRow[] {
            createNewRow(t, 1010L, 1L, 11L, 110L),
            createNewRow(t, 1011L, 1L, 13L, 130L),
            createNewRow(t, 1012L, 1L, (Long)null, 132L),
            createNewRow(t, 1013L, 1L, (Long)null, 132L),
            createNewRow(t, 1014L, 1L, 13L, 133L),
            createNewRow(t, 1015L, 1L, 13L, 134L),
            createNewRow(t, 1016L, 1L, null, 122L),
            createNewRow(t, 1017L, 1L, 14L, 142L),
            createNewRow(t, 1018L, 1L, 30L, 201L),
            createNewRow(t, 1019L, 1L, 30L, null),
            createNewRow(t, 1020L, 1L, 30L, null),
            createNewRow(t, 1021L, 1L, 30L, null),
            createNewRow(t, 1022L, 1L, 30L, 300L),
            createNewRow(t, 1023L, 1L, 40L, 401L)
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
        for (NewRow row : db)
        {
            indexRowMap.put((Long) row.get(0),
                            new TestRow(tRowType,
                                        new Object[] {row.get(1),     // a
                                                      row.get(2),     // b
                                                      row.get(3),     // c
                                                      }));
            
            indexRowWithIdMap.put((Long) row.get(0),
                                  new TestRow(tRowType,
                                              new Object[]{row.get(1),  // a
                                                           row.get(2),  // b
                                                           row.get(3),  // c
                                                           row.get(0)   // id
                                              }));
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
        ValueSource val = indexRow(id).eval(1);
        if(val.isNull())
            return null;

        return (int)indexRow(id).eval(1).getLong();
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        @Ignore
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
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();

        cursor.jump(indexRowWithId(targetId), INDEX_ROW_SELECTOR);

        Row row;
        List<Row> actualRows = new ArrayList<Row>();
        List<ShareHolder<Row>> rowHolders = new ArrayList<ShareHolder<Row>>();
        
        while ((row = cursor.next()) != null)
        {
            // Prevent sharing of rows since verification accumulates them
            actualRows.add(row);
            rowHolders.add(new ShareHolder<Row>(row));
        }
        cursor.close();

        // find the row with given id
        List<Row> expectedRows = new ArrayList<Row>(expected.length);
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
        List<List<Long>> ret = new ArrayList<List<Long>>();
        for (Row row : rows)
        {
            // nulls are allowed
            ArrayList<Long> toLong = new ArrayList<Long>();
            for (int n = 0; n < INDEX_COLUMN_COUNT; ++n)
                addColumn(toLong, row.eval(n));
            
            ret.add(toLong);
        }
        return ret;
    }

    private static void addColumn(List<Long> row, ValueSource v)
    {
        if (v.isNull())
        {
            row.add(null);
            return;
        }

        switch(v.getConversionType())
        {
            case LONG:      row.add(v.getLong());
                            break;
            case INT:       row.add(v.getInt());
                            break;
            case NULL:      row.add(null);
                            break;
            default:        throw new IllegalArgumentException("Unexpected type: " + v.getConversionType());
        }
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

    private TestRow indexRow(long id)
    {
        return indexRowMap.get(id);
    }

    private TestRow indexRowWithId(long id)
    {
        return indexRowWithIdMap.get(id);
    }

    private long[] longs(long... longs)
    {
        return longs;
    }

    private IndexKeyRange bounded(long a, long bLo, boolean loInclusive, long bHi, boolean hiInclusive)
    {
        IndexBound lo = new IndexBound(new TestRow(tRowType, new Object[] {a, bLo}), new SetColumnSelector(0, 1));
        IndexBound hi = new IndexBound(new TestRow(tRowType, new Object[] {a, bHi}), new SetColumnSelector(0, 1));
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
            ordering.append(new FieldExpression(idxRowType, column), asc);
        }
        return ordering;
    }
}
