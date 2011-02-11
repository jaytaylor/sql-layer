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

package com.akiban.server.loader;

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