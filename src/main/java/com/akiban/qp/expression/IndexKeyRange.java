/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.expression;

import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.ColumnSelector;

public class IndexKeyRange
{
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('(');
        if (lo != null) {
            buffer.append(loInclusive() ? ">=" : ">");
            buffer.append(lo.toString());
        }
        buffer.append(',');
        if (hi != null) {
            buffer.append(hiInclusive() ? "<=" : "<");
            buffer.append(hi.toString());
        }
        buffer.append(')');
        return buffer.toString();
    }

    public IndexRowType indexRowType()
    {
        return indexRowType;
    }

    public IndexBound lo()
    {
        return lo;
    }

    public IndexBound hi()
    {
        return hi;
    }

    public boolean loInclusive()
    {
        return loInclusive;
    }

    public boolean hiInclusive()
    {
        return hiInclusive;
    }

    public boolean unbounded()
    {
        return lo == null && hi == null;
    }

    // Indicates a mysql-style index scan, e.g. start at (10, 15) and keep going, even past index records
    // starting with 10.
    public boolean semiBounded()
    {
        return (lo == null || hi == null) && lo != hi;
    }

    public int boundColumns()
    {
        return boundColumns;
    }

    /**
     * Describes a full index scan.
     * @param indexRowType The row type of index keys.
     * @return IndexKeyRange covering all keys of the index.
     */
    public static IndexKeyRange unbounded(IndexRowType indexRowType)
    {
        return new IndexKeyRange(indexRowType);
    }

    /**
     * Describes a range of keys between lo and hi. The bounds are inclusive or not depending on
     * loInclusive and hiInclusive. lo and hi must both be non-null. There are constraints on the bounds:
     * - The ColumnSelectors for lo and hi must select for the same columns.
     * - The selected columns must be leading columns of the index.
     *
     * @param indexRowType The row type of index keys.
     * @param lo           Lower bound of the range.
     * @param loInclusive  True if the lower bound is inclusive, false if exclusive.
     * @param hi           Upper bound of the range.
     * @param hiInclusive  True if the upper bound is inclusive, false if exclusive.
     * @return IndexKeyRange covering the keys lying between lo and hi, subject to the loInclusive and
     * hiInclusive flags.
     */
    public static IndexKeyRange bounded(IndexRowType indexRowType,
                                        IndexBound lo,
                                        boolean loInclusive,
                                        IndexBound hi,
                                        boolean hiInclusive)
    {
        if (lo == null || hi == null) {
            throw new IllegalArgumentException("IndexBound arguments must not be null");
        }
        return new IndexKeyRange(indexRowType, lo, loInclusive, hi, hiInclusive);
    }

    /**
     * Describes all keys in the index starting at or after lo, depending on loInclusive.
     *
     * @param indexRowType The row type of index keys.
     * @param lo           Lower bound of the range.
     * @param loInclusive  True if the lower bound is inclusive, false if exclusive.
     * @return IndexKeyRange covering the keys starting at or after lo.
     */
    public static IndexKeyRange startingAt(IndexRowType indexRowType,
                                           IndexBound lo,
                                           boolean loInclusive)
    {
        if (lo == null) {
            throw new IllegalArgumentException("IndexBound argument must not be null");
        }
        return new IndexKeyRange(indexRowType, lo, loInclusive, null, false);
    }

    /**
     * Describes all keys in the index starting at or after lo, depending on loInclusive.
     *
     * @param indexRowType The row type of index keys.
     * @param hi           Upper bound of the range.
     * @param hiInclusive  True if the upper bound is inclusive, false if exclusive.
     * @return IndexKeyRange covering the keys ending at or before lo.
     */
    public static IndexKeyRange endingAt(IndexRowType indexRowType,
                                         IndexBound hi,
                                         boolean hiInclusive)
    {
        if (hi == null) {
            throw new IllegalArgumentException("IndexBound argument must not be null");
        }
        return new IndexKeyRange(indexRowType, null, false, hi, hiInclusive);
    }

    private IndexKeyRange(IndexRowType indexRowType)
    {
        this.boundColumns = 0;
        this.indexRowType = indexRowType;
        this.lo = null;
        this.loInclusive = false;
        this.hi = null;
        this.hiInclusive = false;
    }

    private IndexKeyRange(IndexRowType indexRowType,
                          IndexBound lo,
                          boolean loInclusive,
                          IndexBound hi,
                          boolean hiInclusive)
    {
        this.boundColumns =
            lo == null
            ? boundColumns(indexRowType, hi) :
            hi == null
            ? boundColumns(indexRowType, lo)
            : boundColumns(indexRowType, lo, hi);
        this.indexRowType = indexRowType;
        this.lo = lo;
        this.loInclusive = loInclusive;
        this.hi = hi;
        this.hiInclusive = hiInclusive;
    }

    private static int boundColumns(IndexRowType indexRowType, IndexBound lo, IndexBound hi)
    {
        ColumnSelector loSelector = lo.columnSelector();
        ColumnSelector hiSelector = hi.columnSelector();
        boolean selected = true;
        int boundColumns = 0;
        for (int i = 0; i < indexRowType.nFields(); i++) {
            if (loSelector.includesColumn(i) != hiSelector.includesColumn(i)) {
                throw new IllegalArgumentException(
                    String.format("IndexBound arguments specify different fields of index %s", indexRowType));
            }
            if (selected) {
                if (loSelector.includesColumn(i)) {
                    boundColumns++;
                } else {
                    selected = false;
                }
            } else {
                if (loSelector.includesColumn(i)) {
                    throw new IllegalArgumentException(
                        String.format("IndexBound arguments for index %s specify non-leading fields", indexRowType));
                }
            }
        }
        assert boundColumns > 0;
        return boundColumns;
    }

    private static int boundColumns(IndexRowType indexRowType, IndexBound bound)
    {
        ColumnSelector selector = bound.columnSelector();
        boolean selected = true;
        int boundColumns = 0;
        for (int i = 0; i < indexRowType.nFields(); i++) {
            if (selected) {
                if (selector.includesColumn(i)) {
                    boundColumns++;
                } else {
                    selected = false;
                }
            } else {
                if (selector.includesColumn(i)) {
                    throw new IllegalArgumentException(
                        String.format("IndexBound arguments for index %s specify non-leading fields", indexRowType));
                }
            }
        }
        assert boundColumns > 0;
        return boundColumns;
    }

    // Object state

    private final IndexRowType indexRowType;
    private final int boundColumns;
    private final IndexBound lo;
    private final boolean loInclusive;
    private final IndexBound hi;
    private final boolean hiInclusive;
}
