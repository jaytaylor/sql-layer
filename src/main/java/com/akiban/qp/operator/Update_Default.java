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

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.row.Row;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.Strings;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import java.util.Collections;
import java.util.List;

/**

 <h1>Overview</h1>

 Provides row update functionality.

 <h1>Arguments</h1>

 <ul>

 <li><b>PhysicalOperator inputOperator:</b> Provides rows to be updated

 <li><b>UpdateFunction updateFunction:</b> specifies which rows are to be updated, and how

 </ul>

 <h1>Behavior</h1>

 For each row from the input operator's cursor, the UpdateOperator
 invokes <i>updateFunction.rowIsSelected</i> to determine if the row
 should be updated. If so, it invokes <i>updateFunction.evaluate</i> to
 get the new version of the row. It then performs the update in an
 unspecified way (in practice, this is currently done via
 <i>StoreAdapater.updateRow</i>, which is implemented by
 <i>PersistitAdapater.updateRow</i>, which invokes
 <i>PersistitStore.updateRow</i>).

 The result of this update is an <i>UpdaateResult</i> instance which summarizes how many rows were updated and how long the operation took.

 <h1>Output</h1>

 N/A (this is an UpdatePlannable, not an Operator).

 <h1>Assumptions</h1>

 Selected rows must have a <i>RowType</i> such
 that <i>rowType.hasUserTable() == true</i>.

 <h1>Performance</h1>

 Updating rows may be slow, especially since indexes are also
 updated. There are several random-access reads and writes involved,
 which depend on the indexes defined for that row type.

 There are potentially ways to optimize this, if we can
 push <i>WHERE</i> clauses down; this would mean we could update some
 indexes as a batch operation, rather than one at a time. This would
 require changes to the API, and is not currently a priority.

 <h1>Memory Requirements</h1>

 Each <i>UpdateFunction.evaluate</i> method may generate a
 new <i>Row</i>.

*/

class Update_Default extends OperatorExecutionBase implements UpdatePlannable {

    // Object interface

    @Override
    public String toString() {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), inputOperator, updateFunction);
    }

    // constructor

    public Update_Default(Operator inputOperator, UpdateFunction updateFunction) {
        ArgumentValidation.notNull("update lambda", updateFunction);
        
        this.inputOperator = inputOperator;
        this.updateFunction = updateFunction;
    }

    // UpdatePlannable interface

    @Override
    public UpdateResult run(Bindings bindings, StoreAdapter adapter) {
        adapter(adapter);
        int seen = 0, modified = 0;
        Cursor inputCursor = null;
        UPDATE_TAP.in();
        try {
            inputCursor = inputOperator.cursor(adapter);
            inputCursor.open(bindings);
            Row oldRow;
            while ((oldRow = inputCursor.next()) != null) {
                checkQueryCancelation();
                ++seen;
                if (updateFunction.rowIsSelected(oldRow)) {
                    Row newRow = updateFunction.evaluate(oldRow, bindings);
                    adapter.updateRow(oldRow, newRow, bindings);
                    ++modified;
                }
            }
        } finally {
            if (inputCursor != null) {
                inputCursor.close();
            }
            UPDATE_TAP.out();
        }
        return new StandardUpdateResult(seen, modified);
    }

    // Plannable interface

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    @Override
    public String describePlan(Operator inputOperator) {
        return inputOperator + Strings.nl() + this;
    }

    // Object state

    private final Operator inputOperator;
    private final UpdateFunction updateFunction;
    private static final InOutTap UPDATE_TAP = Tap.createTimer("operator: update");
    
}
