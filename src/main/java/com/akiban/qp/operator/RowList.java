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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;

import java.util.ArrayList;
import java.util.List;

class RowList
{
    public void add(Row row)
    {
        ensureRoom();
        list.get(count++).set(row);
    }

    public Scan scan()
    {
        scan.reset();
        return scan;
    }

    public void clear()
    {
        for (RowHolder<Row> rowHolder : list) {
            rowHolder.set(null);
        }
        count = 0;
    }

    public RowList()
    {
        for (int i = 0; i < INITIAL_CAPACITY; i++) {
            list.add(new RowHolder<Row>());
        }
    }

    // For use by this class

    private void ensureRoom()
    {
        assert count <= capacity;
        if (count == capacity) {
            int newCapacity = capacity * 2;
            for (int i = capacity; i < newCapacity; i++) {
                list.add(new RowHolder<Row>());
            }
            capacity = newCapacity;
        }
        assert count < capacity;
    }

    // Class state

    private static final int INITIAL_CAPACITY = 10;

    // Object state

    private final List<RowHolder<Row>> list = new ArrayList<RowHolder<Row>>(INITIAL_CAPACITY);
    private int count = 0;
    private int capacity = INITIAL_CAPACITY;
    private final Scan scan = new Scan(); // There should be only one scan of a RowList at a time.

    // Inner classes

    public class Scan
    {
        public Row next()
        {
            Row next = null;
            if (position < count) {
                next = list.get(position++).get();
            }
            return next;
        }

        void reset()
        {
            position = 0;
        }

        private int position = 0;
    }
}
