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
import com.akiban.util.ArgumentValidation;

import java.util.Collections;
import java.util.List;

final class Limit_Default extends PhysicalOperator {

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter) {
        return new Execution(limit(), inputOperator.cursor(adapter));
    }

    // Plannable interface

    @Override
    public RowType rowType() {
        return inputOperator.rowType();
    }

    @Override
    public List<PhysicalOperator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan() {
        return super.describePlan();
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("%s(limit=%d: %s)", getClass().getSimpleName(), limit(), inputOperator);
    }

    // Limit_Default interface

    Limit_Default(PhysicalOperator inputOperator, int limit) {
        ArgumentValidation.isGTE("limit", limit, 0);
        this.limit = limit;
        this.inputOperator = inputOperator;
    }

    public int limit() {
        return limit;
    }

    // Object state

    private final int limit;
    private final PhysicalOperator inputOperator;

    // internal classes

    private static class Execution extends ChainedCursor {

        // Cursor interface

        @Override
        public Row next() {
            if (rowsLeft < 0) {
                return null;
            }
            if (rowsLeft == 0) {
                input.close();
                return null;
            }
            Row row;
            if ((row = input.next()) == null) {
                rowsLeft = -1;
                return null;
            }
            --rowsLeft;
            return row;
        }

        // Execution interface
        Execution(int limit, Cursor input) {
            super(input);
            assert limit >= 0;
            this.rowsLeft = limit;
        }

        // class state

        private int rowsLeft;
    }
}
