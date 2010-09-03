package com.akiban.cserver.loader;

import com.akiban.ais.model.UserTable;

public abstract class GenerateFinalTask extends Task
{
    public final int[] hKeyColumnPositions()
    {
        return hKeyColumnPositions;
    }

    public final int[] columnPositions()
    {
        return columnPositions;
    }

    protected GenerateFinalTask(BulkLoader loader, UserTable table)
    {
        super(loader, table, "$final");
    }

    protected String toString(int[] a)
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('[');
        boolean first = true;
        for (int x : a) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(x);
        }
        buffer.append(']');
        return buffer.toString();
    }

    // Positions of hkey columns in the $final table
    protected int[] hKeyColumnPositions;
    // Positions of columns from the original table in the $final table.
    protected int[] columnPositions;
}