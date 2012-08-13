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

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.operator.Operator;
import org.junit.Test;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.qp.operator.API;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.OverlayingRow;
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

public class IndexScanJumUnboundedITWithNullsIT extends OperatorITBase
{
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
        createIndex("schema", "t", "idx", "a", "b", "c", "id");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new NewRow[] {
            createNewRow(t, 1010L, 1L, 11L, 110L),
            createNewRow(t, 1011L, 1L, 11L, 111L),
            createNewRow(t, 1012L, 1L, (Long)null, 122L),
            createNewRow(t, 1013L, 1L, (Long)null, 122L),
            createNewRow(t, 1014L, 1L, 13L, 122L),
            createNewRow(t, 1015L, 1L, 13L, 123L),
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
                                                      row.get(0)}));  // id
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
        testSkipNulls(1010,
                      b_of(1010), true,
                      b_of(1015), true,
                      getAAAA(),
                      new long[]{1010, 1011, 1014, 1015}); // skip 1012 and 1013
    }

    @Test
    public void testAAAAToNull()
    {
        testSkipNulls(1012, // jump to one of the nulls
                      b_of(1010), true,
                      b_of(1015), true,
                      getAAAA(),
                      new long[] {1012, 1013}); // should see only the nulls, because null < everything
    }

    @Test
    public void testDDDD()
    {
        testSkipNulls(1015,
                      b_of(1010), true,
                      b_of(1015), true,
                      getDDDD(),
                      new long[] {1015, 1014, 1011, 1010}); // skip 1012 and 1013
        
    }

    
    @Test
    public void testDDDDToNull()
    {
        testSkipNulls(1012, // jump to one of the nulls
                      b_of(1010), true,
                      b_of(1015), true,
                      getDDDD(),
                      new long[] {1015, 1014, 1011, 1010, 1013, 1012,}); // should see all the rows because null < everything
    }                                                                    // thus in a DDDD scan, the null rows should appear last

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
        List<Long> actualIds = new ArrayList<Long>();
        while ((row = cursor.next()) != null)
            actualIds.add(row.eval(3).getInt());

        List<Long> expectedIds = new ArrayList<Long>(expected.length);
        for (long val : expected)
            expectedIds.add(val);

        assertEquals(expectedIds, actualIds);
        cursor.close();

    }

    private API.Ordering getAAAA()
    {
        return ordering(A, ASC, B, ASC, C, ASC, ID, ASC);
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

//    private void testRange(API.Ordering ordering,
//                           long idOrdering[],
//                           int nudge,
//                           int lo, boolean loInclusive,
//                           int hi, boolean hiInclusive,
//                           long expectedArs[][])
//    {
//        Operator plan = indexScan_Default(idxRowType, bounded(1, lo, loInclusive, hi, hiInclusive), ordering);
//        Cursor cursor = cursor(plan, queryContext);
//        cursor.open();
//        testJump(cursor,
//                 idOrdering,
//                 nudge,
//                 expectedArs);
//        cursor.close();
//    }

    private void doTestJump(Cursor cursor, long idOrdering[], int nudge, List<List<Long>> expecteds)
    {
        for (int start = 0; start < idOrdering.length; ++start)
        {
            TestRow target = indexRow(idOrdering[start]);
            OverlayingRow nudgedTarget = new OverlayingRow(target);
            nudgedTarget.overlay(3, target.eval(3).getLong() + nudge);
            cursor.jump(nudgedTarget, INDEX_ROW_SELECTOR);
            Row row;
            List<Long> actualIds = new ArrayList<Long>();
            while ((row = cursor.next()) != null)
                actualIds.add(row.eval(3).getInt());
            System.out.println(actualIds);
            //assertEquals(expecteds.get(start), actualIds);
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
        IndexBound lo = new IndexBound(new TestRow(tRowType, new Object[] {a, bLo}), new SetColumnSelector(0, 1));
        IndexBound hi = new IndexBound(new TestRow(tRowType, new Object[] {a, bHi}), new SetColumnSelector(0, 1));
        return IndexKeyRange.bounded(idxRowType, lo, loInclusive, hi, hiInclusive);
    }

    private API.Ordering ordering(Object... ord) // alternating column positions and asc/desc
    {
        API.Ordering ordering = API.ordering();
        int i = 0;
        while (i < ord.length) {
            int column = (Integer) ord[i++];
            boolean asc = (Boolean) ord[i++];
            ordering.append(new FieldExpression(idxRowType, column), asc);
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
    private Map<Long, TestRow> indexRowMap = new HashMap<Long, TestRow>();

}
