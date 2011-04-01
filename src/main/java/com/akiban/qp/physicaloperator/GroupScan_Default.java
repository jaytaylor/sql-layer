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
import com.akiban.qp.BTreeAdapter;
import com.akiban.qp.Cursor;
import com.akiban.qp.GroupCursor;
import com.akiban.qp.HKey;

import java.util.concurrent.atomic.AtomicInteger;

public class GroupScan_Default extends PhysicalOperator
{
    // PhysicalOperator interface

    public OperatorExecution instantiate(BTreeAdapter adapter, OperatorExecution[] ops)
    {
        ops[operatorId] = new Execution(adapter);
        return ops[operatorId];
    }

    // GroupScan_Default interface

    public GroupScan_Default(BTreeAdapter btree, GroupTable groupTable)
    {
        this.btree = btree;
        this.groupTable = groupTable;
    }

    // Object state

    private final BTreeAdapter btree;
    private final GroupTable groupTable;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {
        // OperatorExecution interface

        @Override
        public void bind(Object object)
        {
            cursor.open((HKey) object);
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

        Execution(BTreeAdapter adapter)
        {
            super(adapter);
            this.cursor = btree.newGroupCursor(groupTable);
        }

        // Object state

        private final GroupCursor cursor;
    }
}
