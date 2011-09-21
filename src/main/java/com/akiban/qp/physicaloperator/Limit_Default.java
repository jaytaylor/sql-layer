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
import java.util.Set;

final class Limit_Default extends PhysicalOperator {

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter) {
        return new Execution(skip(), limit(), inputOperator.cursor(adapter));
    }

    // Plannable interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
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
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        str.append("(");
        if (skip > 0) {
            str.append(String.format("skip=%d", skip));
        }
        if (limit < Integer.MAX_VALUE) {
            str.append(String.format("limit=%d", skip));
        }
        str.append(": ");
        str.append(inputOperator);
        str.append(")");
        return str.toString();
    }

    // Limit_Default interface

    Limit_Default(PhysicalOperator inputOperator, int limit) {
        this(inputOperator, 0, limit);
    }

    Limit_Default(PhysicalOperator inputOperator, int skip, int limit) {
        ArgumentValidation.isGTE("skip", skip, 0);
        ArgumentValidation.isGTE("limit", limit, 0);
        this.skip = skip;
        this.limit = limit;
        this.inputOperator = inputOperator;
    }

    public int skip() {
        return skip;
    }
    public int limit() {
        return limit;
    }

    // Object state

    private final int skip, limit;
    private final PhysicalOperator inputOperator;

    // internal classes

    private static class Execution extends ChainedCursor {

        // Cursor interface

        @Override
        public Row next() {
            Row row;
            while (skipLeft > 0) {
                if ((row = input.next()) == null) {
                    skipLeft = 0;
                    rowsLeft = -1;
                    return null;
                }
                skipLeft--;
            }
            if (rowsLeft < 0) {
                return null;
            }
            if (rowsLeft == 0) {
                input.close();
                return null;
            }
            if ((row = input.next()) == null) {
                rowsLeft = -1;
                return null;
            }
            --rowsLeft;
            return row;
        }

        // Execution interface
        Execution(int skip, int limit, Cursor input) {
            super(input);
            assert skip >= 0;
            assert limit >= 0;
            this.skipLeft = skip;
            this.rowsLeft = limit;
        }

        // class state

        private int skipLeft, rowsLeft;
    }
}
