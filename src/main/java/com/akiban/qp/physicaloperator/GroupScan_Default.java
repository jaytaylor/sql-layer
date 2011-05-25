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

package com.akiban.qp.physicaloperator;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.Row;

class GroupScan_Default extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getClass().getSimpleName());
        buffer.append('(');
        buffer.append(groupTable.getName().getTableName());
        if (indexKeyRange != null) {
            buffer.append(" range");
        }
        buffer.append(' ');
        buffer.append(limit);
        buffer.append(')');
        return buffer.toString();
    }

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, indexKeyRange);
    }

    // GroupScan_Default interface

    public GroupScan_Default(GroupTable groupTable, Limit limit, IndexKeyRange indexKeyRange)
    {
        this.groupTable = groupTable;
        this.limit = limit;
        this.indexKeyRange = indexKeyRange;
    }

    // Object state

    private final GroupTable groupTable;
    private final Limit limit;
    private final IndexKeyRange indexKeyRange;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {

        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            cursor.open(bindings);
        }

        @Override
        public boolean next()
        {
            boolean next = cursor.next();
            if (next) {
                Row row = cursor.currentRow();
                outputRow(row);
                if (limit.limitReached(row)) {
                    close();
                    next = false;
                }
            } else {
                close();
                next = false;
            }
            return next;
        }

        @Override
        public void close()
        {
            outputRow(null);
            cursor.close();
        }

        // Execution interface

        Execution(StoreAdapter adapter, IndexKeyRange indexKeyRange)
        {
            this.cursor = adapter.newGroupCursor(groupTable, indexKeyRange);
        }

        // Object state

        private final Cursor cursor;
    }
}
