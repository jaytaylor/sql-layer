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

public class UniqueIndexScanJumpUnboundedWithNullsIT extends OperatorITBase
{
     // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;

    private static final int COLUMN_COUNT = 3;

    private static final boolean ASC = true;
    private static final boolean DESC = false;

    private static final SetColumnSelector INDEX_ROW_SELECTOR = new SetColumnSelector(0, 1, 2);

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private Map<Long, TestRow> indexRowMap = new HashMap<Long, TestRow>();

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
            createNewRow(t, 1018L, 1L, 20L, 201L),
            createNewRow(t, 1019L, 1L, 30L, null),
            createNewRow(t, 1020L, 1L, 30L, null),
            createNewRow(t, 1021L, 1L, 30L, null),
            createNewRow(t, 1022L, 1L, 30L, 300L),
            createNewRow(t, 1023L, 1L, 40L, 401L)
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
        for (NewRow row : db) {
            indexRowMap.put((Long) row.get(0),
                            new TestRow(tRowType,
                                        new Object[] {row.get(1),     // a
                                                      row.get(2),     // b
                                                      row.get(3)      // c
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
    public void testAAA()
    {
        testSkipNulls(1010,
                      getAAA(),
                      new long[]{1010, 1011, 1014, 1015, 1017, 1018, 1019, 1020, 1021, 1022, 1023}); // skip 1012, 1013 and 1016
    }

    @Test
    public void testAAAToNull()
    {
        testSkipNulls(1012, // jump to one of the nulls
                      getAAA(),
                      new long[] {1012, 1013, 1016, 1010, 1011, 1014, 1015, 1017, 1018, 1019, 1020, 1021, 1022, 1023}); // should see the nulls first, because null < everything
    }

    @Test
    public void testDDD()
    {
        testSkipNulls(1015,
                      getDDD(),
                      new long[] {1015, 1014, 1011, 1010, 1016, 1013, 1012}); // should see the nulls last because null < everything
        
    }

    @Test
    public void testDDDToMinNull()
    {
        testSkipNulls(1012,
                       getDDD(),
                       new long[] {1012, 1013, 1016});
    }

    @Test
    public void testDDDToMediumNull()
    {
        // currently failing
        // doens't return any row

        testSkipNulls(1013, // jump to one of the nulls
                      getDDD(),
                      new long[] {1013, 1012, 1016}); // should only see all three [1, null, 122]
    }                                                 // (The use of (1013, 1012, 1016) is for demonstartive purpose
                                                      // (Any id could've been used as long as their index row is [1, null, 122]
    @Test
    public void testDDDToMaxNull()
    {
        testSkipNulls(1016,
                      getDDD(),
                      new long[] {1016, 1013, 1012}); 
    }
 
    
    @Test
    public void testDDDToFirstNull()
    {
        testSkipNulls(1019, // jump to the first null
                      getDDD(),
                      new long[] {1021, 1020, 1019, 1018, 1017, 1015, 1014, 1011, 1010, 1016, 1013, 1012});
    }

    @Test
    public void testDDDToMiddleNull()
    {
        testSkipNulls(1020, // jump to the middle null
                      getDDD(),
                      new long[] {1021, 1020, 1019, 1018, 1017, 1015, 1014, 1011, 1010, 1016, 1013, 1012});
    }

    @Test
    public void testDDDToLastNull()
    {
        testSkipNulls(1021, // jump to the first null
                      getDDD(),
                      new long[] {1021, 1020, 1019, 1018, 1017, 1015, 1014, 1011, 1010, 1016, 1013, 1012});
    }
    
    @Ignore
    @Test
    public void testAAD()
    {
        // currently failing
        // throw IndexOutOfBoundException

        testSkipNulls(1014,
                      getAAD(),
                      new long[] {1014, 1015, 1017}); // skips 1016, which is a null
    }
    
    //TODO: add more test****() if needed

    private void testSkipNulls(long targetId,                  // location to jump to
                               API.Ordering ordering,          
                               long expected[])
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering);
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();
        cursor.jump(indexRow(targetId), INDEX_ROW_SELECTOR);

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
            
            for (int n = 0; n < COLUMN_COUNT; ++n)
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
            ordering.append(new FieldExpression(idxRowType, column), asc);
        }
        return ordering;
    }
}
