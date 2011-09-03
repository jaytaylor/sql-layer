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

import java.util.List;

import com.akiban.qp.row.Row;
import com.akiban.sql.optimizer.ExpressionRow;

public class ValuesScan_Default extends PhysicalOperator {

    @Override
    protected Cursor cursor(StoreAdapter adapter) {
        return new Execution(rows);
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName()  + rows;
    }

    public ValuesScan_Default (List<ExpressionRow> rows) 
    {
        this.rows = rows;
    }
    
    private List<ExpressionRow> rows;
    
    private static class Execution implements Cursor
    {
        private List<ExpressionRow> rows;
        private int index;
        public Execution (List<ExpressionRow> rows) {
            this.rows = rows;
        }

        @Override
        public void close() {
            index = rows.size() + 1;
        }

        @Override
        public Row next() {
            if (index < rows.size()) { 
                return rows.get(index++);
            } else { 
                return null;
            }
        }

        @Override
        public void open(Bindings bindings) {
            index = 0;
        }
    }
}
