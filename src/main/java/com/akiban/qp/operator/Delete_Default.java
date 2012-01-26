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
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

/**

 <h1>Overview</h1>

 The Delete_Default deletes rows from a given table. Every row provided
 by the input operator is sent to the <i>StoreAdapter#deleteRow()</i>
 method to be removed from the table.

 <h1>Arguments</h1>

 <ul>

 <li><b>input:</b> the input operator supplying rows to be deleted.

 </ul>

 <h1>Behaviour</h1>

 Rows supplied by the input operator are deleted from the underlying
 data store through the StoreAdapter interface.

 <h1>Output</h1>

 The operator does not create a cursor returning rows. Instead it
 supplies a run() method which returns an <i>UpdateResult</i>

 <h1>Assumptions</h1>

 The rows provided by the input operator includes all of the columns
 for the HKEY to allow the persistit layer to lookup the row in the
 btree to remove it. Failure results in a RowNotFoundException being
 thrown and the operation aborted.

 The operator assumes (but does not require) that all rows provided are
 of the same RowType.

 The Delete_Default operator assumes (and requires) the input row types
 be of a UserTableRowType, and not any derived type. This can't be
 enforced by the constructor because <i>PhysicalOperator#rowType()</i>
 isn't implemented for all operators.

 <h1>Performance</h1>

 Deletion assumes the data store needs to alter the underlying storage
 system, including any system change log. This requires multiple IOs
 per operation.

 <h1>Memory Requirements</h1>

 Each row is individually processed.

 */

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
    public UpdateResult run(QueryContext context) {
        context(context);
        int seen = 0, modified = 0;
        DELETE_TAP.in();
        Cursor inputCursor = inputOperator.cursor(context);
        inputCursor.open();
        try {
            Row oldRow;
            while ((oldRow = inputCursor.next()) != null) {
                checkQueryCancelation();
                ++seen;
                adapter().deleteRow(oldRow);
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
    private static final InOutTap DELETE_TAP = Tap.createTimer("operator: delete");

}
