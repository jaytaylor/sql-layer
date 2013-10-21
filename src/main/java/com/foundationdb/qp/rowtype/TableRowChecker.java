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

package com.foundationdb.qp.rowtype;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NotNullViolationException;

import java.util.BitSet;

public class TableRowChecker implements ConstraintChecker
{
    @Override
    public void checkConstraints(Row row) throws InvalidOperationException
    {
        for (int f = 0; f < fields; f++) {
            if (notNull.get(f) && isNull(row, f)) {
                TableName tableName = table.getName();
                throw new NotNullViolationException(tableName.getSchemaName(),
                                                    tableName.getTableName(),
                                                    table.getColumnsIncludingInternal().get(f).getName());
            }
        }
    }

    private boolean isNull(Row row, int f) {
        return row.value(f).isNull();
    }

    public TableRowChecker(RowType rowType)
    {
        assert rowType.hasTable() : rowType;
        fields = rowType.nFields();
        table = rowType.table();
        notNull = table.notNull();
    }

    public TableRowChecker(Table table)
    {
        this.table = table;
        fields = table.getColumnsIncludingInternal().size();
        notNull = table.notNull();
    }

    private final Table table;
    private final int fields;
    private final BitSet notNull;
}
