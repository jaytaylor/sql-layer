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

import java.util.Collections;
import java.util.List;

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.row.Row;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.Strings;
import com.akiban.util.Tap;

public final class Insert_Default implements UpdatePlannable {

    public Insert_Default(PhysicalOperator inputOperator, UpdateFunction updateFunction) {
        ArgumentValidation.notNull("insert lambda", updateFunction);
        
        this.inputOperator = inputOperator;
        this.insertFunction = updateFunction;
    }

    @Override
    public UpdateResult run(Bindings bindings, StoreAdapter adapter) {
        int seen = 0, modified = 0;
        INSERT_TAP.in();
        //long start = System.currentTimeMillis();
        Cursor inputCursor = inputOperator.cursor(adapter);
        inputCursor.open(bindings);
        try {
            Row oldRow;
            while ((oldRow = inputCursor.next()) != null) {
                ++seen;
                if (insertFunction.rowIsSelected(oldRow)) {
                    Row newRow = insertFunction.evaluate(oldRow, bindings);
                    adapter.writeRow(newRow, bindings);
                    ++modified;
                }
            }
        } finally {
            inputCursor.close();
            INSERT_TAP.out();
        }
        //long end = System.currentTimeMillis();
        return new StandardUpdateResult(INSERT_TAP.getDuration(), seen, modified);
    }

    @Override
    public String describePlan() {
        return describePlan(inputOperator);
    }

    @Override
    public String describePlan(PhysicalOperator inputOperator) {
        return inputOperator + Strings.nl() + this;
    }

    @Override
    public List<PhysicalOperator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String toString() {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), inputOperator, insertFunction);
    }

    private final PhysicalOperator inputOperator;
    private final UpdateFunction insertFunction;
    private static final Tap.InOutTap INSERT_TAP = Tap.createTimer("operator: insert");

}
