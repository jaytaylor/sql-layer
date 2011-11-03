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

import java.util.Collections;
import java.util.List;

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.row.Row;
import com.akiban.util.Strings;
import com.akiban.util.Tap;

class Delete_Default extends OperatorExecutionBase implements UpdatePlannable {

    // constructor

    public Delete_Default(Operator inputOperator) {
        this.inputOperator = inputOperator;
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), inputOperator);
    }

    @Override
    public UpdateResult run(Bindings bindings, StoreAdapter adapter) {
        adapter(adapter);
        int seen = 0, modified = 0;
        DELETE_TAP.in();
        Cursor inputCursor = inputOperator.cursor(adapter);
        inputCursor.open(bindings);
        try {
            Row oldRow;
            while ((oldRow = inputCursor.next()) != null) {
                checkQueryCancelation();
                ++seen;
                adapter.deleteRow(oldRow, bindings);
                ++modified;
            }
        } finally {
            inputCursor.close();
            DELETE_TAP.out();
        }
        return new StandardUpdateResult(seen, modified);
    }

    @Override
    public String describePlan() {
        return describePlan(inputOperator);
    }

    @Override
    public String describePlan(Operator inputOperator) {
        return inputOperator + Strings.nl() + this;
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    private final Operator inputOperator;
    private static final Tap.InOutTap DELETE_TAP = Tap.createTimer("operator: delete");

}
