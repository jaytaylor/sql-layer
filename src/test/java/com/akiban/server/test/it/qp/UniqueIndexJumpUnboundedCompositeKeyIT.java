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

import java.util.Arrays;
import java.lang.Long;
import com.akiban.util.ShareHolder;
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

    private static final int INDEX_COLUMN_COUNT = 3;

    private static final boolean ASC = true;
    private static final boolean DESC = false;

    private static final SetColumnSelector INDEX_ROW_SELECTOR = new SetColumnSelector(0, 1, 2, 3, 4);

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
            "id1 int not null",
            "id2 int not null",
            "a int",
            "b int",
            "c int",
            "PRIMARY KEY (id1, id2)");
        
        createUniqueIndex("schema", "t", "idx", "a", "b", "c");
        
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "a", "b", "c");
        db = new NewRow[] {
            createNewRow(t, 0L, 1010L, 1L, 11L, 110L),
            createNewRow(t, 1L, 1011L, 1L, 13L, 130L),
            createNewRow(t, 2L, 1012L, 1L, (Long)null, 132L),
            createNewRow(t, 2L, 1013L, 1L, (Long)null, 132L),
            createNewRow(t, 3L, 1013L, 1L, (Long)null, 132L),
            createNewRow(t, 4L, 1014L, 1L, 13L, 133L),
            createNewRow(t, 5L, 1015L, 1L, 13L, 134L),
            createNewRow(t, 6L, 1016L, 1L, null, 122L),
            createNewRow(t, 7L, 1017L, 1L, 14L, 142L),
//            createNewRow(t, 1018L, 1L, 30L, 201L),
//            createNewRow(t, 1019L, 1L, 30L, null),
//            createNewRow(t, 1020L, 1L, 30L, null),
//            createNewRow(t, 1021L, 1L, 30L, null),
//            createNewRow(t, 1022L, 1L, 30L, 300L),
//            createNewRow(t, 1023L, 1L, 40L, 401L)
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
        for (NewRow row : db)
        {
            indexRowMap.put(id((Long)row.get(0), (Long)row.get(1)),
                            new TestRow(tRowType,
                                        new Object[] {row.get(2),     // a
                                                      row.get(3),     // b
                                                      row.get(4),     // c
                                                      }));
            
            indexRowWithIdMap.put(id((Long)row.get(0), (Long)row.get(1)),
                                  new TestRow(tRowType,
                                              new Object[]{row.get(2),  // a
                                                           row.get(3),  // b
                                                           row.get(4),  // c
                                                           row.get(0),  // id1
                                                           row.get(1)   // id2
                                              }));
        }
    }

    

    // --- Start generated
/*
1
AAAAA:
{6, 1016}, 
{2, 1012}, 
{2, 1013}, 
{3, 1013}, 
{0, 1010}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{7, 1017}, 

*/

    @Test
    public void testAAAAA_6_1016()
    {
        testUnbounded(6, 1016,
                      getAAAAA(),
                      new long [][]{
                                    {6, 1016}, 
                                    {2, 1012}, 
                                    {2, 1013}, 
                                    {3, 1013}, 
                                    {0, 1010}, 
                                    {1, 1011}, 
                                    {4, 1014}, 
                                    {5, 1015}, 
                                    {7, 1017}, 
                      });
    }
     
    @Test
    public void testAAAAA_2_1012()
    {
        testUnbounded(2, 1012,
                      getAAAAA(),
                      new long [][] {
                                     {2, 1012},
                                     {2, 1013},
                                     {3, 1013}, 
                                     {0, 1010},
                                     {1, 1011},
                                     {4, 1014},
                                     {5, 1015},
                                     {7, 1017}}
                      );
    }
    
    @Test
    public void testAAAAA_2_1013()
    {
        testUnbounded(2, 1013,
                      getAAAAA(),
                      new long [][]{
                                    {2, 1013}, 
                                    {3, 1013}, 
                                    {0, 1010}, 
                                    {1, 1011}, 
                                    {4, 1014}, 
                                    {5, 1015}, 
                                    {7, 1017}, 
                      });
    }

    @Test
    public void testAAAAA_3_1013()
    {
        testUnbounded(3, 1013,
                      getAAAAA(),
                      new long [][] {
                                    {3, 1013}, 
                                    {0, 1010}, 
                                    {1, 1011}, 
                                    {4, 1014}, 
                                    {5, 1015}, 
                                    {7, 1017}, 
                      });
    }
/*
2
AAAAD:
{6, 1016}, 
{2, 1013}, 
{2, 1012}, 
{3, 1013}, 
{0, 1010}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{7, 1017}, 

*/
    @Test
    public void testAAAAD_6_1016()
    {
        // currently failing 
        
        testUnbounded(6, 1016,
                      getAAAAD(),
                      new long [][]{
                                    {6, 1016}, 
                                    {2, 1013}, 
                                    {2, 1012}, 
                                    {3, 1013}, 
                                    {0, 1010}, 
                                    {1, 1011}, 
                                    {4, 1014}, 
                                    {5, 1015}, 
                                    {7, 1017},
                      });
    }

    @Test
    public void testAAAAD_2_1013()
    {
        // currently failing 
        
        testUnbounded(2, 1013,
                      getAAAAD(),
                      new long [][]{
                                    {2, 1013}, 
                                    {2, 1012}, 
                                    {3, 1013}, 
                                    {0, 1010}, 
                                    {1, 1011}, 
                                    {4, 1014}, 
                                    {5, 1015}, 
                                    {7, 1017},
                      });
    }
        
    @Test
    public void testAAAAD_2_1012()
    {
        
        // currently failing

        testUnbounded(2, 1012,
                      getAAAAD(),
                      new long [][] {
                                    {2, 1012}, 
                                    {3, 1013}, 
                                    {0, 1010}, 
                                    {1, 1011}, 
                                    {4, 1014}, 
                                    {5, 1015}, 
                                    {7, 1017}, 
                      });
    }

/*
3
AAADA:
{6, 1016}, 
{3, 1013}, 
{2, 1012}, 
{2, 1013}, 
{0, 1010}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{7, 1017}, 

*/


/*
4
AAADD:
{6, 1016}, 
{3, 1013}, 
{2, 1013}, 
{2, 1012}, 
{0, 1010}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{7, 1017}, 

*/


/*
5
AADAA:
{2, 1012}, 
{2, 1013}, 
{3, 1013}, 
{6, 1016}, 
{0, 1010}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{7, 1017}, 

*/


/*
6
AADAD:
{2, 1013}, 
{2, 1012}, 
{3, 1013}, 
{6, 1016}, 
{0, 1010}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{7, 1017}, 

*/


/*
7
AADDA:
{3, 1013}, 
{2, 1012}, 
{2, 1013}, 
{6, 1016}, 
{0, 1010}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{7, 1017}, 

*/


/*
8
AADDD:
{3, 1013}, 
{2, 1013}, 
{2, 1012}, 
{6, 1016}, 
{0, 1010}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{7, 1017}, 

*/


/*
9
ADAAA:
{7, 1017}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{0, 1010}, 
{6, 1016}, 
{2, 1012}, 
{2, 1013}, 
{3, 1013}, 

*/


/*
10
ADAAD:
{7, 1017}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{0, 1010}, 
{6, 1016}, 
{2, 1013}, 
{2, 1012}, 
{3, 1013}, 

*/


/*
11
ADADA:
{7, 1017}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{0, 1010}, 
{6, 1016}, 
{3, 1013}, 
{2, 1012}, 
{2, 1013}, 

*/


/*
12
ADADD:
{7, 1017}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{0, 1010}, 
{6, 1016}, 
{3, 1013}, 
{2, 1013}, 
{2, 1012}, 

*/


/*
13
ADDAA:
{7, 1017}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{0, 1010}, 
{2, 1012}, 
{2, 1013}, 
{3, 1013}, 
{6, 1016}, 

*/


/*
14
ADDAD:
{7, 1017}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{0, 1010}, 
{2, 1013}, 
{2, 1012}, 
{3, 1013}, 
{6, 1016}, 

*/


/*
15
ADDDA:
{7, 1017}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{0, 1010}, 
{3, 1013}, 
{2, 1012}, 
{2, 1013}, 
{6, 1016}, 

*/


/*
16
ADDDD:
{7, 1017}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{0, 1010}, 
{3, 1013}, 
{2, 1013}, 
{2, 1012}, 
{6, 1016}, 

*/


/*
17
DAAAA:
{6, 1016}, 
{2, 1012}, 
{2, 1013}, 
{3, 1013}, 
{0, 1010}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{7, 1017}, 

*/


/*
18
DAAAD:
{6, 1016}, 
{2, 1013}, 
{2, 1012}, 
{3, 1013}, 
{0, 1010}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{7, 1017}, 

*/


/*
19
DAADA:
{6, 1016}, 
{3, 1013}, 
{2, 1012}, 
{2, 1013}, 
{0, 1010}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{7, 1017}, 

*/


/*
20
DAADD:
{6, 1016}, 
{3, 1013}, 
{2, 1013}, 
{2, 1012}, 
{0, 1010}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{7, 1017}, 

*/


/*
21
DADAA:
{2, 1012}, 
{2, 1013}, 
{3, 1013}, 
{6, 1016}, 
{0, 1010}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{7, 1017}, 

*/


/*
22
DADAD:
{2, 1013}, 
{2, 1012}, 
{3, 1013}, 
{6, 1016}, 
{0, 1010}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{7, 1017}, 

*/


/*
23
DADDA:
{3, 1013}, 
{2, 1012}, 
{2, 1013}, 
{6, 1016}, 
{0, 1010}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{7, 1017}, 

*/


/*
24
DADDD:
{3, 1013}, 
{2, 1013}, 
{2, 1012}, 
{6, 1016}, 
{0, 1010}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{7, 1017}, 

*/


/*
25
DDAAA:
{7, 1017}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{0, 1010}, 
{6, 1016}, 
{2, 1012}, 
{2, 1013}, 
{3, 1013}, 

*/


/*
26
DDAAD:
{7, 1017}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{0, 1010}, 
{6, 1016}, 
{2, 1013}, 
{2, 1012}, 
{3, 1013}, 

*/


/*
27
DDADA:
{7, 1017}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{0, 1010}, 
{6, 1016}, 
{3, 1013}, 
{2, 1012}, 
{2, 1013}, 

*/


/*
28
DDADD:
{7, 1017}, 
{1, 1011}, 
{4, 1014}, 
{5, 1015}, 
{0, 1010}, 
{6, 1016}, 
{3, 1013}, 
{2, 1013}, 
{2, 1012}, 

*/


/*
29
DDDAA:
{7, 1017}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{0, 1010}, 
{2, 1012}, 
{2, 1013}, 
{3, 1013}, 
{6, 1016}, 

*/


/*
30
DDDAD:
{7, 1017}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{0, 1010}, 
{2, 1013}, 
{2, 1012}, 
{3, 1013}, 
{6, 1016}, 

*/


/*
31
DDDDA:
{7, 1017}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{0, 1010}, 
{3, 1013}, 
{2, 1012}, 
{2, 1013}, 
{6, 1016}, 

*/


/*
32
DDDDD:
{7, 1017}, 
{5, 1015}, 
{4, 1014}, 
{1, 1011}, 
{0, 1010}, 
{3, 1013}, 
{2, 1013}, 
{2, 1012}, 
{6, 1016}, 

*/
    
    // --- DONE generated
    
    private void testUnbounded(long targetId1,                  // location to jump to
                                long targetId2,
                                API.Ordering ordering,          
                                long expected[][])
    {
        doTest(targetId1, targetId2, unbounded(), ordering, expected);
    }

     private void testBounded(long targetId1,                  // location to jump to
                              long targetId2,
                              int bLo, boolean lowInclusive,  // lower bound
                              int bHi, boolean hiInclusive,   // upper bound
                              API.Ordering ordering,          
                              long expected[][])
    {
        doTest(targetId1, targetId2, bounded(1, bLo, lowInclusive, bHi, hiInclusive), ordering, expected);
    }

    private void doTest(long targetId1,                  // location to jump to
                        long targetId2,
                        IndexKeyRange range,
                        API.Ordering ordering,          
                        long expected[][])
    {
        Operator plan = indexScan_Default(idxRowType, range, ordering);
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();


        cursor.jump(indexRowWithId(targetId1, targetId2), INDEX_ROW_SELECTOR);

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

        // check the list of rows
        checkRows(actualRows, expected);
    }
    
    private void checkRows(List<Row> actual, long expected[][])
    {
        List<List<Long>> actualList = new ArrayList<List<Long>>();
        for (Row row : actual)
            actualList.add(Arrays.asList(row.eval(ID1).getInt(),
                                         row.eval(ID2).getInt()));
        
        List<List<Long>> expectedList = new ArrayList<List<Long>>();
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

    private TestRow indexRow(long id1, long id2)
    {
        return indexRowMap.get(id(id1, id2));
    }

    private TestRow indexRow(long val)
    {
        return indexRowMap.get(val);
    }

    private TestRow indexRowWithId(long id1, long id2)
    {
        return indexRowWithIdMap.get(id(id1, id2));
    }

    private TestRow indexRowWithId(long val)
    {
        return indexRowWithIdMap.get(val);
    }
    
    private long id(long id1, long id2)
    {
        return id2 << 4 | id1;
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
