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

import java.util.List;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;

public abstract class GenerateParentTask extends Task
{
    public GenerateParentTask(BulkLoader loader, UserTable table)
    {
        super(loader, table, "$parent");
    }

    public List<Column> pkColumns()
    {
        return pkColumns;
    }

    protected void pkColumns(List<Column> pkColumns)
    {
        if (this.pkColumns != null) {
            throw new BulkLoader.InternalError(pkColumns.toString());
        }
        this.pkColumns = pkColumns;
    }

    private List<Column> pkColumns;
}