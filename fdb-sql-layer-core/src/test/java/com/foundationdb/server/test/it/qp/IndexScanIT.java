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

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.value.ValueSource;

import org.junit.Ignore;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertEquals;

public class IndexScanIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        use(db);
    }

    // IndexKeyRange argument checking

    @Test(expected = IllegalArgumentException.class)
    public void testNullLoBound()
    {
        IndexKeyRange.bounded(itemIidIndexRowType, null, false, iidBound(0), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullHiBound()
    {
        IndexKeyRange.bounded(itemOidIidIndexRowType, iidBound(0), false, null, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullLoAndHiSelectorsMismatch()
    {
        IndexBound loBound = new IndexBound(row(itemOidIidIndexRowType, 0, 0), new SetColumnSelector(0));
        IndexBound hiBound = new IndexBound(row(itemOidIidIndexRowType, 0, 0), new SetColumnSelector(1));
        IndexKeyRange.bounded(itemOidIidIndexRowType, loBound, false, hiBound, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectorForNonLeadingColumn()
    {
        IndexBound loBound = new IndexBound(row(itemOidIidIndexRowType, 0, 0), new SetColumnSelector(1));
        IndexBound hiBound = new IndexBound(row(itemOidIidIndexRowType, 0, 0), new SetColumnSelector(1));
        IndexKeyRange.bounded(itemOidIidIndexRowType, loBound, false, hiBound, false);
    }

    @Test
    public void testLegalSelector()
    {
        IndexBound loBound = new IndexBound(row(itemOidIidIndexRowType, 0, 0), new SetColumnSelector(0));
        IndexBound hiBound = new IndexBound(row(itemOidIidIndexRowType, 0, 0), new SetColumnSelector(0));
        IndexKeyRange.bounded(itemOidIidIndexRowType, loBound, false, hiBound, false);
    }

    // Check invalid ranges (can only be done when cursor is opened)

    @Test(expected = IllegalArgumentException.class)
    public void testMoreThanOneInequalityUnidirectional()
    {
        IndexBound loBound = new IndexBound(row(itemOidIidIndexRowType, 10, 10), new SetColumnSelector(0, 1));
        IndexBound hiBound = new IndexBound(row(itemOidIidIndexRowType, 20, 20), new SetColumnSelector(0, 1));
        IndexKeyRange keyRange = IndexKeyRange.bounded(itemOidIidIndexRowType, loBound, false, hiBound, false);
        Operator indexScan = indexScan_Default(itemOidIidIndexRowType, false, keyRange);
        String[] expected = new String[]{};
        compareRenderedHKeys(expected, cursor(indexScan, queryContext, queryBindings));
    }

    @Test(expected = IllegalArgumentException.class)
    @Ignore("Mixed order not supported")
    public void testMoreThanOneInequalityMixedMode()
    {
        IndexBound loBound = new IndexBound(row(itemOidIidIndexRowType, 10, 10), new SetColumnSelector(0, 1));
        IndexBound hiBound = new IndexBound(row(itemOidIidIndexRowType, 20, 20), new SetColumnSelector(0, 1));
        IndexKeyRange keyRange = IndexKeyRange.bounded(itemOidIidIndexRowType, loBound, false, hiBound, false);
        API.Ordering ordering = new API.Ordering();
        ordering.append(ExpressionGenerators.field(itemOidIidIndexRowType, 0), true);
        ordering.append(ExpressionGenerators.field(itemOidIidIndexRowType, 1), false);
        Operator indexScan = indexScan_Default(itemOidIidIndexRowType, keyRange, ordering);
        String[] expected = new String[]{};
        compareRenderedHKeys(expected, cursor(indexScan, queryContext, queryBindings));
    }

    //

    @Test
    public void testExactMatchAbsent()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(299, true, 299, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testExactMatchPresent()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(212, true, 212, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 212)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testEmptyRange()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(218, true, 219, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{};
        compareRenderedHKeys(expected, cursor);
    }

    // The next three tests are light tests of unbounded multi-column index scanning, including mixed-mode.
    // More serious tests of index scans with mixed-mode and bounds are in IndexScanBoundedIT

    @Test
    public void testFullScan()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType);
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{
            hkey(1, 11, 111),
            hkey(1, 11, 112),
            hkey(1, 12, 121),
            hkey(1, 12, 122),
            hkey(2, 21, 211),
            hkey(2, 21, 212),
            hkey(2, 22, 221),
            hkey(2, 22, 222),
        };
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testFullScanReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true);
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{
            hkey(2, 22, 222),
            hkey(2, 22, 221),
            hkey(2, 21, 212),
            hkey(2, 21, 211),
            hkey(1, 12, 122),
            hkey(1, 12, 121),
            hkey(1, 11, 112),
            hkey(1, 11, 111),
        };
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    @Ignore("Mixed order not supported")
    public void testMixedMode()
    {
        API.Ordering ordering = new API.Ordering();
        ordering.append(field(itemOidIidIndexRowType, 0), true);
        ordering.append(field(itemOidIidIndexRowType, 1), false);
        Operator indexScan = indexScan_Default(itemOidIidIndexRowType,
                                               IndexKeyRange.unbounded(itemOidIidIndexRowType),
                                               ordering,
                                               itemOidIidIndexRowType.tableType());
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{
            hkey(1, 11, 112),
            hkey(1, 11, 111),
            hkey(1, 12, 122),
            hkey(1, 12, 121),
            hkey(2, 21, 212),
            hkey(2, 21, 211),
            hkey(2, 22, 222),
            hkey(2, 22, 221),
        };
        compareRenderedHKeys(expected, cursor);
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
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(121, true, 211, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 121), hkey(1, 12, 122), hkey(2, 21, 211)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMatchHiInclusiveMiss()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(212, true, 223, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 212), hkey(2, 22, 221), hkey(2, 22, 222)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMatchHiExclusiveMatch()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(212, true, 222, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 212), hkey(2, 22, 221)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMatchHiExclusiveMiss()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(212, true, 223, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 212), hkey(2, 22, 221), hkey(2, 22, 222)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMissHiInclusiveMatch()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(100, true, 121, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 11, 111), hkey(1, 11, 112), hkey(1, 12, 121)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMissHiInclusiveMiss()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(100, true, 125, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 11, 111), hkey(1, 11, 112), hkey(1, 12, 121), hkey(1, 12, 122)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMissHiExclusiveMatch()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(100, true, 122, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 11, 111), hkey(1, 11, 112), hkey(1, 12, 121)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMissHiExclusiveMiss()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(100, true, 125, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 11, 111), hkey(1, 11, 112), hkey(1, 12, 121), hkey(1, 12, 122)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMatchHiInclusiveMatch()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(121, false, 211, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 122), hkey(2, 21, 211)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMatchHiInclusiveMiss()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(212, false, 223, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 22, 221), hkey(2, 22, 222)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMatchHiExclusiveMatch()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(212, false, 222, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 22, 221)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMatchHiExclusiveMiss()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(212, false, 223, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 22, 221), hkey(2, 22, 222)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMissHiInclusiveMatch()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(100, false, 121, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 11, 111), hkey(1, 11, 112), hkey(1, 12, 121)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMissHiInclusiveMiss()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(100, false, 125, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 11, 111), hkey(1, 11, 112), hkey(1, 12, 121), hkey(1, 12, 122)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMissHiExclusiveMatch()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(100, false, 122, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 11, 111), hkey(1, 11, 112), hkey(1, 12, 121)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMissHiExclusiveMiss()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, false, iidKeyRange(100, false, 125, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 11, 111), hkey(1, 11, 112), hkey(1, 12, 121), hkey(1, 12, 122)};
        compareRenderedHKeys(expected, cursor);
    }

    // Reverse versions of above tests

    @Test
    public void testExactMatchAbsentReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(299, true, 299, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testExactMatchPresentReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(212, true, 212, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 212)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testEmptyRangeReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(218, true, 219, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMatchHiInclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(121, true, 211, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 211), hkey(1, 12, 122), hkey(1, 12, 121)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMatchHiInclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(212, true, 223, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 22, 222), hkey(2, 22, 221), hkey(2, 21, 212)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMatchHiExclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(212, true, 222, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 22, 221), hkey(2, 21, 212)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMatchHiExclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(212, true, 223, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 22, 222), hkey(2, 22, 221), hkey(2, 21, 212)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMissHiInclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, true, 121, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMissHiInclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, true, 125, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 122), hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMissHiExclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, true, 122, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoInclusiveMissHiExclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, true, 125, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 122), hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMatchHiInclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(121, false, 211, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 21, 211), hkey(1, 12, 122)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMatchHiInclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(212, false, 223, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 22, 222), hkey(2, 22, 221)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMatchHiExclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(212, false, 222, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 22, 221)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMatchHiExclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(212, false, 223, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(2, 22, 222), hkey(2, 22, 221)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMissHiInclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, false, 121, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMissHiInclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, false, 125, true));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 122), hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMissHiExclusiveMatchReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, false, 122, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testLoExclusiveMissHiExclusiveMissReverse()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, false, 125, false));
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        String[] expected = new String[]{hkey(1, 12, 122), hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
        compareRenderedHKeys(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        Operator indexScan = indexScan_Default(itemIidIndexRowType, true, iidKeyRange(100, false, 125, false));
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public boolean hKeyComparison()
            {
                return true;
            }

            @Override
            public String[] firstExpectedHKeys()
            {
                return new String[]{hkey(1, 12, 122), hkey(1, 12, 121), hkey(1, 11, 112), hkey(1, 11, 111)};
            }
        };
        testCursorLifecycle(indexScan, testCase);
    }

    // Inspired by bug 898013
    @Test
    public void testAliasingOfPersistitIndexRowFields()
    {
        Operator indexScan = indexScan_Default(itemOidIidIndexRowType, false, null);
        Cursor cursor = cursor(indexScan, queryContext, queryBindings);
        cursor.openTopLevel();
        Row row = cursor.next();
        // Get and checking each field should work
        assertEquals(11, row.value(0).getInt32());
        assertEquals(111, row.value(1).getInt32());
        // Getting all value sources and then using them should also work
        ValueSource v0 = row.value(0);
        ValueSource v1 = row.value(1);
        assertEquals(11, v0.getInt32());
        assertEquals(111, v1.getInt32());
    }

    // For use by this class

    private IndexKeyRange iidKeyRange(int lo, boolean loInclusive, int hi, boolean hiInclusive)
    {
        return IndexKeyRange.bounded(itemIidIndexRowType, iidBound(lo), loInclusive, iidBound(hi), hiInclusive);
    }

    private IndexBound iidBound(int iid)
    {
        return new IndexBound(row(itemIidIndexRowType, iid), new SetColumnSelector(0));
    }

    private String hkey(int cid, int oid, int iid)
    {
        return String.format("{1,(long)%s,2,(long)%s,3,(long)%s}", cid, oid, iid);
    }
}
