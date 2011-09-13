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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;

public class ValuesScan_Default extends PhysicalOperator {

    // PhysicalOperator interface

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    protected Cursor cursor(StoreAdapter adapter) {
        return new Execution(rows);
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName()  + rows;
    }

    public ValuesScan_Default (Collection<? extends Row> rows, RowType rowType) {
        this.rows = new ArrayDeque<Row> (rows);
        this.rowType = rowType;
    }

    private final Deque<Row> rows;
    private final RowType rowType;
    
    private static class Execution implements Cursor
    {
        private final Deque<Row> rows;
        private final Deque<Row> cursorRows;

        public Execution (Deque<Row> rows) {
            this.rows = rows;
            this.cursorRows = new ArrayDeque<Row>();
        }

        @Override
        public void close() {
            cursorRows.clear();
        }

        @Override
        public Row next() {
            return cursorRows.poll();
        }

        @Override
        public void open(Bindings bindings) {
            cursorRows.clear();
            cursorRows.addAll(rows);
        }
    }
}
