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

import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
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
 * This differs from UniqueIndexScanJumpBoundedWithNullsIT in that each index row in this
 * test contains a unique id
 * 
 * (Open to suggestion on a better name)
 */
public class UniqueIndexScanJumpBoundedWithNulls2IT extends OperatorITBase
{
     // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int ID = 3;
    
    private static final int INDEX_COLUMN_COUNT = 3;

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
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "a", "b", "c");
        db = new NewRow[] {
            createNewRow(t, 1010L, 1L, 11L, 110L),
            createNewRow(t, 1011L, 1L, 11L, 111L),
            createNewRow(t, 1012L, 1L, (Long)null, 122L),
            createNewRow(t, 1013L, 1L, (Long)null, 122L),
            createNewRow(t, 1014L, 1L, 13L, 132L),
            createNewRow(t, 1015L, 1L, 13L, 133L),
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
    private int b_of(long id)
    {
        return (int)indexRow(id).eval(1).getLong();
    }

    @Test
    public void testAAAA()
    {
        test(1019, // jump to the first null
                      b_of(1018), true,
                      b_of(1021), true,
                      getAAAA(),
                      new long[] {1019, 1020, 1021, 1018, 1022});
        
        test(1020, // jump to the second null
                      b_of(1018), true,
                      b_of(1021), true,
                      getAAAA(),
                      new long[] {1020, 1021, 1018, 1022});
        
        test(1021, // jump to the last null
                      b_of(1018), true,
                      b_of(1021), true,
                      getAAAA(),
                      new long[] {1021, 1018, 1022});
    }

    @Test
    public void testDDDD()
    {
        // currently failing
        // 3 cases all doesn't return any row

        test(1019,
             b_of(1018), true,
             b_of(1021), true,
             getDDDD(),
             new long[] {1019});

        test(1020,
             b_of(1018), true,
             b_of(1021), true,
             getDDDD(),
             new long[] {1020, 1019});

        test(1021,
             b_of(1018), true,
             b_of(1021), true,
             getDDDD(),
             new long[] {1021, 1020, 1019});
    }

    @Test
    public void testAAAD()
    {
        // currently failing

        // 'correct odering': 
        // 1021, 1020, 1019, 1018, 1022
        // --->

        test(1019,
             b_of(1018), true,
             b_of(1021), true,
             getAAAD(),
             new long[] {1019, 1018, 1022});
     
        test(1020,
             b_of(1018), true,
             b_of(1021), true,
             getAAAD(),
             new long[] {1020, 1019, 1018, 1022});
        
        test(1021,
             b_of(1018), true,
             b_of(1021), true,
             getAAAD(),
             new long[] {1021, 1020, 1019, 1018, 1022});
    }

    //TODO: add more test****()
    
    private void test(long targetId,                  // location to jump to
                       int bLo, boolean lowInclusive,  // lower bound
                       int bHi, boolean hiInclusive,   // upper bound
                       API.Ordering ordering,          
                       long expected[])
    {
        Operator plan = indexScan_Default(idxRowType, bounded(1, bLo, lowInclusive, bHi, hiInclusive), ordering);
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
            expectedRows.add(indexRow(val));

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
