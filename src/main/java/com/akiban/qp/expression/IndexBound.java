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

import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.ColumnSelector;

public class IndexBound
{
    public String toString()
    {
        return String.format("%s", row);
    }

    public RowBase row()
    {
        return row;
    }

    public ColumnSelector columnSelector()
    {
        return columnSelector;
    }

    public IndexBound(RowBase row, ColumnSelector columnSelector)
    {
        this.row = row;
        this.columnSelector = columnSelector;
    }

    // Object state

    private final RowBase row;
    private final ColumnSelector columnSelector;
}
