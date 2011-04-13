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
import com.akiban.qp.row.HKey;

class GroupScan_Default extends PhysicalOperator
{
    // PhysicalOperator interface

    public OperatorExecution instantiate(StoreAdapter adapter, OperatorExecution[] ops)
    {
        ops[operatorId] = new Execution(adapter);
        return ops[operatorId];
    }

    // GroupScan_Default interface

    public GroupScan_Default(StoreAdapter store, GroupTable groupTable)
    {
        this.store = store;
        this.groupTable = groupTable;
    }

    // Object state

    private final StoreAdapter store;
    private final GroupTable groupTable;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {
        // OperatorExecution interface

        @Override
        public void bind(IndexKeyRange keyRange)
        {
            cursor.bind(keyRange);
        }

        @Override
        public void bind(HKey hKey)
        {
            cursor.bind(hKey);
        }

        // Cursor interface

        @Override
        public void open()
        {
            cursor.open();
        }

        @Override
        public boolean next()
        {
            boolean next = cursor.next();
            if (next) {
                outputRow(cursor.currentRow());
            } else {
                close();
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

        Execution(StoreAdapter adapter)
        {
            super(adapter);
            this.cursor = store.newGroupCursor(groupTable);
        }

        // Object state

        private final GroupCursor cursor;
    }
}
