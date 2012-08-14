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

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static junit.framework.Assert.assertEquals;

// Like IndexScanIT, but testing unique indexes and the handling of duplicate nulls

public class UniqueIndexScanIT extends OperatorITBase
{
    @Before
    public void before()
    {
        item = createTable(
            "schema", "item",
            "id int not null",
            "x int", // copy of id or null
            "y int", // copy of id or null
            "primary key (id)");
        createUniqueIndex("schema", "item", "idx_x", "x");
        createUniqueIndex("schema", "item", "idx_y", "y");
        createUniqueIndex("schema", "item", "idx_xy", "x", "y");
        schema = new Schema(rowDefCache().ais());
        itemRowType = schema.userTableRowType(userTable(item));
        xIndexRowType = indexType(item, "x");
        yIndexRowType = indexType(item, "y");
        xyIndexRowType = indexType(item, "x", "y");
        group = groupTable(item);
        db = new NewRow[]{
            createNewRow(item, 1L, 10L, 10L),
            createNewRow(item, 2L, 20L, 20L),
            createNewRow(item, 3L, 30L, 30L),
            createNewRow(item, 4L, 40L, 40L),
            createNewRow(item, 5L, 50L, 50L),
            createNewRow(item, 6L, 60L, 60L),
            createNewRow(item, 7L, 70L, null),
            createNewRow(item, 8L, 80L, null),
            createNewRow(item, 9L, null, 90L),
            createNewRow(item, 10L, null, 100L),
            createNewRow(item, 11L, null, null),
            createNewRow(item, 12L, null, null),
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
    }

    @Test
    public void testDuplicateNulls()
    {
        {
            Operator indexScan = indexScan_Default(xIndexRowType);
            compareRenderedHKeys(
                hkeys(9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8),
                cursor(indexScan, queryContext));
        }
        {
            Operator indexScan = indexScan_Default(yIndexRowType);
            compareRenderedHKeys(
                hkeys(7, 8, 11, 12, 1, 2, 3, 4, 5, 6, 9, 10),
                cursor(indexScan, queryContext));
        }
        {
            Operator indexScan = indexScan_Default(xyIndexRowType);
            compareRenderedHKeys(
                hkeys(11, 12, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8),
                cursor(indexScan, queryContext));
        }
    }

    @Test
    public void testExactMatchAbsent()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 5, true, 5, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(), cursor);
    }

    @Test
    public void testExactMatchPresent()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 10, true, 10, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(1), cursor);
    }

    @Test
    public void testEmptyRange()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 11, true, 12, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(), cursor);
    }

    @Test
    public void testStartAtNull()
    {
        {
            Operator indexScan = indexScan_Default(xIndexRowType, false, startAtNull(xIndexRowType, true, 60, true));
            Cursor cursor = cursor(indexScan, queryContext);
            compareRenderedHKeys(hkeys(9, 10, 11, 12, 1, 2, 3, 4, 5, 6), cursor);
        }
        {
            Operator indexScan = indexScan_Default(yIndexRowType, false, startAtNull(xIndexRowType, true, 60, true));
            Cursor cursor = cursor(indexScan, queryContext);
            compareRenderedHKeys(hkeys(7, 8, 11, 12, 1, 2, 3, 4, 5, 6), cursor);
        }
    }

    @Test
    public void testStartAfterNull()
    {
        {
            Operator indexScan = indexScan_Default(xIndexRowType, false, startAtNull(xIndexRowType, false, 60, true));
            Cursor cursor = cursor(indexScan, queryContext);
            dump(cursor);
            // compareRenderedHKeys(hkeys(1, 2, 3, 4, 5, 6), cursor);
        }
        {
            Operator indexScan = indexScan_Default(yIndexRowType, false, startAtNull(xIndexRowType, false, 60, true));
            Cursor cursor = cursor(indexScan, queryContext);
            compareRenderedHKeys(hkeys(1, 2, 3, 4, 5, 6), cursor);
        }
    }

    // Naming scheme for next tests:
    // testLoABHiCD
    // A: Inclusive/Exclusive for lo bound
    // B: Match/Miss indicates whether the lo bound matches or misses an actual value in the db
    // C: Inclusive/Exclusive for hi bound
    // D: Match/Miss indicates whether the hi bound matches or misses an actual value in the db

    @Test
    public void testLoInclusiveMatchHiInclusiveMatch()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 20, true, 40, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(2, 3, 4), cursor);
    }

    @Test
    public void testLoInclusiveMatchHiInclusiveMiss()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 20, true, 45, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(2, 3, 4), cursor);
    }

    @Test
    public void testLoInclusiveMatchHiExclusiveMatch()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 20, true, 50, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(2, 3, 4), cursor);
    }

    @Test
    public void testLoInclusiveMatchHiExclusiveMiss()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 20, true, 45, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(2, 3, 4), cursor);
    }

    @Test
    public void testLoInclusiveMissHiInclusiveMatch()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 5, true, 50, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(1, 2, 3, 4, 5), cursor);
    }

    @Test
    public void testLoInclusiveMissHiInclusiveMiss()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 5, true, 55, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(1, 2, 3, 4, 5), cursor);
    }

    @Test
    public void testLoInclusiveMissHiExclusiveMatch()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 5, true, 60, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(1, 2, 3, 4, 5), cursor);
    }

    @Test
    public void testLoInclusiveMissHiExclusiveMiss()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 5, true, 55, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(1, 2, 3, 4, 5), cursor);
    }

    @Test
    public void testLoExclusiveMatchHiInclusiveMatch()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 60, false, 70, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(7), cursor);
    }

    @Test
    public void testLoExclusiveMatchHiInclusiveMiss()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 60, false, 75, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(7), cursor);
    }

    @Test
    public void testLoExclusiveMatchHiExclusiveMatch()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 60, false, 80, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(7), cursor);
    }

    @Test
    public void testLoExclusiveMatchHiExclusiveMiss()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 60, false, 75, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(7), cursor);
    }

    @Test
    public void testLoExclusiveMissHiInclusiveMatch()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 45, false, 60, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(5, 6), cursor);
    }

    @Test
    public void testLoExclusiveMissHiInclusiveMiss()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 45, false, 65, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(5, 6), cursor);
    }

    @Test
    public void testLoExclusiveMissHiExclusiveMatch()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 45, false, 70, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(5, 6), cursor);
    }

    @Test
    public void testLoExclusiveMissHiExclusiveMiss()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, false, keyRange(xIndexRowType, 45, false, 65, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(hkeys(5, 6), cursor);
    }

    // Reverse versions of above tests

    @Test
    public void testDuplicateNullsReverse()
    {
        {
            Operator indexScan = indexScan_Default(xIndexRowType, true);
            compareRenderedHKeys(
                reverse(hkeys(9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8)),
                cursor(indexScan, queryContext));
        }
        {
            Operator indexScan = indexScan_Default(yIndexRowType, true);
            compareRenderedHKeys(
                reverse(hkeys(7, 8, 11, 12, 1, 2, 3, 4, 5, 6, 9, 10)),
                cursor(indexScan, queryContext));
        }
        {
            Operator indexScan = indexScan_Default(xyIndexRowType, true);
            compareRenderedHKeys(
                reverse(hkeys(11, 12, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8)),
                cursor(indexScan, queryContext));
        }
    }

    @Test
    public void testExactMatchAbsentReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 5, true, 5, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys()), cursor);
    }

    @Test
    public void testExactMatchPresentReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 10, true, 10, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(1)), cursor);
    }

    @Test
    public void testEmptyRangeReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 11, true, 12, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys()), cursor);
    }

    @Test
    public void testStartAtNullReverse()
    {
        {
            Operator indexScan = indexScan_Default(xIndexRowType, true, startAtNull(xIndexRowType, true, 60, true));
            Cursor cursor = cursor(indexScan, queryContext);
            compareRenderedHKeys(reverse(hkeys(9, 10, 11, 12, 1, 2, 3, 4, 5, 6)), cursor);
        }
        {
            Operator indexScan = indexScan_Default(yIndexRowType, true, startAtNull(yIndexRowType, true, 60, true));
            Cursor cursor = cursor(indexScan, queryContext);
            compareRenderedHKeys(reverse(hkeys(7, 8, 11, 12, 1, 2, 3, 4, 5, 6)), cursor);
        }
    }

    @Test
    public void testStartAfterNullReverse()
    {
        {
            Operator indexScan = indexScan_Default(xIndexRowType, true, startAtNull(xIndexRowType, false, 60, true));
            Cursor cursor = cursor(indexScan, queryContext);
            compareRenderedHKeys(reverse(hkeys(1, 2, 3, 4, 5, 6)), cursor);
        }
        {
            Operator indexScan = indexScan_Default(yIndexRowType, true, startAtNull(yIndexRowType, false, 60, true));
            Cursor cursor = cursor(indexScan, queryContext);
            compareRenderedHKeys(reverse(hkeys(1, 2, 3, 4, 5, 6)), cursor);
        }
    }

    // Naming scheme for next tests:
    // testLoABHiCD
    // A: Inclusive/Exclusive for lo bound
    // B: Match/Miss indicates whether the lo bound matches or misses an actual value in the db
    // C: Inclusive/Exclusive for hi bound
    // D: Match/Miss indicates whether the hi bound matches or misses an actual value in the db

    @Test
    public void testLoInclusiveMatchHiInclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 20, true, 40, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(2, 3, 4)), cursor);
    }

    @Test
    public void testLoInclusiveMatchHiInclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 20, true, 45, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(2, 3, 4)), cursor);
    }

    @Test
    public void testLoInclusiveMatchHiExclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 20, true, 50, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(2, 3, 4)), cursor);
    }

    @Test
    public void testLoInclusiveMatchHiExclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 20, true, 45, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(2, 3, 4)), cursor);
    }

    @Test
    public void testLoInclusiveMissHiInclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 5, true, 50, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(1, 2, 3, 4, 5)), cursor);
    }

    @Test
    public void testLoInclusiveMissHiInclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 5, true, 55, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(1, 2, 3, 4, 5)), cursor);
    }

    @Test
    public void testLoInclusiveMissHiExclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 5, true, 60, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(1, 2, 3, 4, 5)), cursor);
    }

    @Test
    public void testLoInclusiveMissHiExclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 5, true, 55, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(1, 2, 3, 4, 5)), cursor);
    }

    @Test
    public void testLoExclusiveMatchHiInclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 60, false, 70, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(7)), cursor);
    }

    @Test
    public void testLoExclusiveMatchHiInclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 60, false, 75, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(7)), cursor);
    }

    @Test
    public void testLoExclusiveMatchHiExclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 60, false, 80, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(7)), cursor);
    }

    @Test
    public void testLoExclusiveMatchHiExclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 60, false, 75, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(7)), cursor);
    }

    @Test
    public void testLoExclusiveMissHiInclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 45, false, 60, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(5, 6)), cursor);
    }

    @Test
    public void testLoExclusiveMissHiInclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 45, false, 65, true));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(5, 6)), cursor);
    }

    @Test
    public void testLoExclusiveMissHiExclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 45, false, 70, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(5, 6)), cursor);
    }

    @Test
    public void testLoExclusiveMissHiExclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(xIndexRowType, true, keyRange(xIndexRowType, 45, false, 65, false));
        Cursor cursor = cursor(indexScan, queryContext);
        compareRenderedHKeys(reverse(hkeys(5, 6)), cursor);
    }

    // For use by this class

    private IndexKeyRange startAtNull(IndexRowType indexRowType, boolean loInclusive, int hi, boolean hiInclusive)
    {
        return IndexKeyRange.bounded(indexRowType,
                                     new IndexBound(row(indexRowType, new Object[]{null}), COLUMN_0),
                                     loInclusive,
                                     new IndexBound(row(indexRowType, hi), COLUMN_0),
                                     hiInclusive);
    }

    private IndexKeyRange keyRange(IndexRowType indexRowType, int lo, boolean loInclusive, int hi, boolean hiInclusive)
    {
        return IndexKeyRange.bounded(indexRowType,
                                     new IndexBound(row(indexRowType, lo), COLUMN_0),
                                     loInclusive,
                                     new IndexBound(row(indexRowType, hi), COLUMN_0),
                                     hiInclusive);
    }

    private String[] hkeys(int... ids)
    {
        String[] hkeys = new String[ids.length];
        for (int i = 0; i < ids.length; i++) {
            hkeys[i] = hkey(ids[i]);
        }
        return hkeys;
    }

    private String hkey(int id)
    {
        return String.format("{1,(long)%s}", id);
    }

    private String[] reverse(String[] a)
    {
        for (int i = 0; i < a.length / 2; i++) {
            String x = a[i];
            a[i] = a[a.length - 1 - i];
            a[a.length - 1 - i] = x;
        }
        return a;
    }

    private static ColumnSelector COLUMN_0 = new SetColumnSelector(0);

    private IndexRowType xIndexRowType;
    private IndexRowType yIndexRowType;
    private IndexRowType xyIndexRowType;
    private GroupTable group;

}
