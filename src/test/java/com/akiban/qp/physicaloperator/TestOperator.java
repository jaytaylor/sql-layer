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

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;

import java.util.ArrayDeque;
import java.util.Deque;

public final class TestOperator extends PhysicalOperator {
    
    // PhysicalOperator interface

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    protected Cursor cursor(StoreAdapter adapter) {
        return new InternalCursor(rows);
    }

    public TestOperator(RowsBuilder rowsBuilder) {
        this.rows = new ArrayDeque<Row>(rowsBuilder.rows());
        this.rowType = rowsBuilder.rowType();
    }

    private final Deque<Row> rows;
    private final RowType rowType;

    // object state

    private static class InternalCursor implements Cursor {
        @Override
        public void open(Bindings bindings) {
            cursorValues.clear();
            cursorValues.addAll(allValues);
        }

        @Override
        public Row next() {
            return cursorValues.poll();
        }

        @Override
        public void close() {
            cursorValues.clear();
        }

        private InternalCursor(Deque<Row> rows) {
            allValues = new ArrayDeque<Row>(rows);
        }

        private final Deque<Row> allValues;
        private final Deque<Row> cursorValues = new ArrayDeque<Row>();
    }
}
