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

public class UniqueIndexScanJumpBoundedWithNullsIT extends OperatorITBase
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
            createNewRow(t, 1017L, 1L, 14L, 142L)
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
        for (NewRow row : db) {
            indexRowMap.put((Long) row.get(0),
                            new TestRow(tRowType,
                                        new Object[] {row.get(1),     // a
                                                      row.get(2),     // b
                                                      row.get(3),     // c
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
        // currently failing
        // for some reason, the rows returned by this jump is 
        // a bunch of [1, 14, 142] (corresponding to id = 1017)
        
        testSkipNulls(1010,
                      b_of(1010), true,
                      b_of(1015), true,
                      getAAA(),
                      new long[]{1010, 1011, 1014, 1015}); // skip 1012 and 1013
    }

    @Test
    public void testAAAToMinNull()
    {
        // same failures as testAAAA()

        testSkipNulls(1012, // jump to one of the nulls
                      b_of(1010), true,
                      b_of(1015), true,
                      getAAA(),
                      new long[] {1012, 1013, 1016, 1010, 1011, 1014, 1015}); // should see everything
    }                                                                         // with nulls appearing first

    @Test
    public void testDDD()
    {
        // currently failing
        // This doesn't even return the correct number of rows
        // Only 3 rows are returned, while the expected is 4
        // And the 3 rows returned are identical: [1, null, 122]

        testSkipNulls(1015,
                      b_of(1010), true,
                      b_of(1015), true,
                      getDDDD(),
                      new long[] {1015, 1014, 1011, 1010}); // skip 1012 and 1013
        
    }

    
    @Test
    public void testDDDToMiddleNull()
    {
        // currently failing
        // Doesn't return any row

        testSkipNulls(1013, // jump to one of the nulls
                      b_of(1010), true,
                      b_of(1015), true,
                      getDDDD(),
                      new long[] {1012}); //should see only the min null (1012)
    }

    @Test
    public void testDDDToMaxNull()
    {
        testSkipNulls(1016,
                      b_of(1015), false,
                      b_of(1017), true,
                      getDDDD(),
                      new long[] {}); 
    }
    
    @Test
    public void testAAD()
    {
        // same failures as testAAAA()

        testSkipNulls(1014,
                      b_of(1010), true,
                      b_of(1017), true,
                      getAAD(),
                      new long[] {1014, 1015, 1017}); // skips 1016, which is a null
    }

    //TODO: add more test****()

    private void testSkipNulls(long targetId,                  // location to jump to
                               int bLo, boolean lowInclusive,  // lower bound
                               int bHi, boolean hiInclusive,   // upper bound
                               API.Ordering ordering,          
                               long expected[])
    {
        Operator plan = indexScan_Default(idxRowType, bounded(1, bLo, lowInclusive, bHi, hiInclusive), ordering);
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
    

    private API.Ordering getDDAA()
    {
        return ordering(A, DESC, B, DESC, C, ASC);
    }

    private API.Ordering getDDAD()
    {
        return ordering(A, DESC, B, DESC, C, ASC);
    }

    private API.Ordering getDDDA()
    {
         return ordering(A, DESC, B, DESC, C, DESC);
    }

    private API.Ordering getDDDD()
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
