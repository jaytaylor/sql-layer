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

package com.foundationdb.ais.model;

/**
 * IndexToHKey is an interface useful in constructing HKey values from an index row.
 * There are two types of entries, ordinal values and index fields. An ordinal identifies
 * a user table. An index field selects a field within the index row.
 */
public class IndexToHKey
{
    public IndexToHKey(int[] ordinals, int[] indexRowPositions)
    {
        if (ordinals.length != indexRowPositions.length) {
            throw new IllegalArgumentException("All arrays must be of equal length: " +
                                               ordinals.length + ", " +
                                               indexRowPositions.length);
        }
        this.ordinals = ordinals;
        this.indexRowPositions = indexRowPositions;
    }

    public boolean isOrdinal(int index)
    {
        return ordinals[index] >= 0;
    }

    public int getOrdinal(int index)
    {
        return ordinals[index];
    }

    public int getIndexRowPosition(int index)
    {
        return indexRowPositions[index];
    }

    public int getLength()
    {
        return ordinals.length;
    }

    // If set, value >= 0, the ith field of the hkey is this ordinal
    private final int[] ordinals;
    // If set, value >= 0, the ith field of the hkey is at this position in the index row
    private final int[] indexRowPositions;
}
