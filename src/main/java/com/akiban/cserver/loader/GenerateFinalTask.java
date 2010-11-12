package com.akiban.cserver.loader;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;

public abstract class GenerateFinalTask extends Task
{
    public final int[] hKeyColumnPositions()
    {
        if (hKeyColumnPositions == null) {
            hKeyColumnPositions = new int[hKey().size()];
            int p = 0;
            for (Column hKeyColumn : hKey()) {
                int hKeyColumnPosition = columns().indexOf(hKeyColumn);
                if (hKeyColumnPosition == -1) {
                    throw new BulkLoader.InternalError(hKeyColumn.toString());
                }
                hKeyColumnPositions[p++] = hKeyColumnPosition;
            }
        }
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

    // Positions of columns from the original table in the $final table.
    protected int[] columnPositions;
    // Positions of hkey columns in the $final table
    private int[] hKeyColumnPositions;
}