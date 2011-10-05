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

    /**
     * Describes a range of keys between lo and hi. The bounds are inclusive or not depending on
     * loInclusive and hiInclusive. If lo is null, then the lower bound is less than all values, and
     * loInclusive must be false. If hi is null, then the upper bound is greater than all values, and
     * hiInclusive must be false.
     * @param lo Lower bound of the range.
     * @param loInclusive True if the lower bound is inclusive, false if exclusive.
     * @param hi Upper bound of the range.
     * @param hiInclusive True if the upper bound is inclusive, false if exclusive.
     */
    public IndexKeyRange(IndexRowType indexRowType,
                         IndexBound lo,
                         boolean loInclusive,
                         IndexBound hi,
                         boolean hiInclusive)
    {
        if (lo == null && loInclusive) {
            throw new IllegalArgumentException();
        }
        if (hi == null && hiInclusive) {
            throw new IllegalArgumentException();
        }
        this.indexRowType = indexRowType;
        this.lo = lo;
        this.loInclusive = loInclusive;
        this.hi = hi;
        this.hiInclusive = hiInclusive;
    }

    // Object state

    private final IndexRowType indexRowType;
    private final IndexBound lo;
    private final boolean loInclusive;
    private final IndexBound hi;
    private final boolean hiInclusive;
}
