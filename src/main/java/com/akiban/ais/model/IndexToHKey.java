
package com.akiban.ais.model;

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
