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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.ArgumentValidation;

public class ValuesScan_Default extends Operator
{

    // Operator interface

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    protected Cursor cursor(StoreAdapter adapter) {
        return new Execution(adapter, rows);
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName()  + rows;
    }

    public ValuesScan_Default (Collection<? extends BindableRow> bindableRows, RowType rowType) {
        this.rows = new ArrayList<BindableRow>(bindableRows);
        this.rowType = rowType;
    }

    private final Collection<? extends BindableRow> rows;
    private final RowType rowType;
    
    private static class Execution extends OperatorExecutionBase implements Cursor
    {
        private final Collection<? extends BindableRow> rows;
        private Iterator<? extends BindableRow> iter;
        private Bindings bindings;

        public Execution (StoreAdapter adapter, Collection<? extends BindableRow> rows) {
            super(adapter);
            this.rows = rows;
        }

        @Override
        public void close() {
            iter = null;
            bindings = null;
        }

        @Override
        public Row next() {
            if (iter != null && iter.hasNext()) {
                return iter.next().bind(bindings, adapter);
            } else {
                return null;
            }
        }

        @Override
        public void open(Bindings bindings) {
            ArgumentValidation.notNull("bindings", bindings);
            this.bindings = bindings;
            iter = rows.iterator();
        }
    }
}
