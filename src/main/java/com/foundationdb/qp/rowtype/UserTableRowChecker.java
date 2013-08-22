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
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NotNullViolationException;

import java.util.BitSet;

public class UserTableRowChecker implements ConstraintChecker
{
    @Override
    public void checkConstraints(Row row, boolean usePValues) throws InvalidOperationException
    {
        for (int f = 0; f < fields; f++) {
            if (notNull.get(f) && isNull(row, f, usePValues)) {
                TableName tableName = table.getName();
                throw new NotNullViolationException(tableName.getSchemaName(),
                                                    tableName.getTableName(),
                                                    table.getColumnsIncludingInternal().get(f).getName());
            }
        }
    }

    private boolean isNull(Row row, int f, boolean usePValues) {
        return usePValues
                ? row.pvalue(f).isNull()
                : row.eval(f).isNull();
    }

    public UserTableRowChecker(RowType rowType)
    {
        assert rowType.hasUserTable() : rowType;
        fields = rowType.nFields();
        table = rowType.userTable();
        notNull = table.notNull();
    }

    public UserTableRowChecker(UserTable userTable)
    {
        fields = userTable.getColumnsIncludingInternal().size();
        table = userTable;
        notNull = table.notNull();
    }

    private final int fields;
    private final UserTable table;
    private final BitSet notNull;
}
