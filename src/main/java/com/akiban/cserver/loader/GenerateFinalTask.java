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

    // Positions of hkey columns in the $final table
    protected int[] hKeyColumnPositions;
    // Positions of columns from the original table in the $final table.
    protected int[] columnPositions;
}