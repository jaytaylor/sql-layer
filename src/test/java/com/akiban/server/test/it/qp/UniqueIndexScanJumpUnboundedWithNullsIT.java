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
    private static final int ID = 3;
    private static final boolean ASC = true;
    private static final boolean DESC = false;
    private static final SetColumnSelector INDEX_ROW_SELECTOR = new SetColumnSelector(0, 1, 2, 3);

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
        createUniqueIndex("schema", "t", "idx", "a", "b", "c", "id");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
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
                      getAAAA(),
                      new long[]{1010, 1011, 1014, 1015, 1017}); // skip 1012, 1013 and 1016
    }

    @Test
    public void testAAAAToNull()
    {
        testSkipNulls(1012, // jump to one of the nulls
                      getAAAA(),
                      new long[] {1012, 1013, 1016, 1010, 1011, 1014, 1015, 1017}); // should see the nulls first, because null < everything
    }

    @Test
    public void testDDDD()
    {
        testSkipNulls(1015,
                      getDDDD(),
                      new long[] {1015, 1014, 1011, 1010, 1016, 1013, 1012}); // should see the nulls last because null < everything
        
    }

    
    @Test
    public void testDDDDToMiddleNull()
    {
        testSkipNulls(1013, // jump to one of the nulls
                      getDDDD(),
                      new long[] {1013, 1012}); // should only see 1013 and 1012 (not 1016)
    }

    @Test
    public void testDDDDToMaxNull()
    {
        testSkipNulls(1016,
                      getDDDD(),
                      new long[] {1016, 1013, 1012}); 
    }
    
    @Test
    public void testDDDDToMinNull()
    {
        testSkipNulls(1012,
                       getDDDD(),
                       new long[] {1012});
    }
    
    @Test
    public void testAAAD()
    {
        testSkipNulls(1014,
                      getAAAD(),
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

    private API.Ordering getAAAD()
    {
        return ordering(A, ASC, B, ASC, C, ASC, ID, DESC);
    }
    
    
    private API.Ordering getDADD()
    {
        return ordering(A, DESC, B, ASC, C, DESC, ID, DESC);
    }

    private API.Ordering getDDAA()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID, ASC);
    }

    private API.Ordering getDDAD()
    {
        return ordering(A, DESC, B, DESC, C, ASC, ID, DESC);
    }

    private API.Ordering getDDDA()
    {
         return ordering(A, DESC, B, DESC, C, DESC, ID, ASC);
    }

    private API.Ordering getDDDD()
    {
        return ordering(A, DESC, B, DESC, C, DESC, ID, DESC);
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
