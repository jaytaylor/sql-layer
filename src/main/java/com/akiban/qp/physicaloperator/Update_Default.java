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
import com.akiban.util.ArgumentValidation;

import java.util.Collections;
import java.util.List;

public final class Update_Default extends PhysicalOperator {

    // Object interface

    @Override
    public String toString() {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), inputOperator, updateLambdaBindable);
    }

    // constructor

    public Update_Default(PhysicalOperator inputOperator, Bindable<UpdateLambda> updateLambdaBindable) {
        ArgumentValidation.notNull("update lambda", updateLambdaBindable);
        if (!inputOperator.cursorAbilitiesInclude(CursorAbility.MODIFY)) {
            throw new IllegalArgumentException("input operator must be modifiable: " + inputOperator.getClass());
        }
        
        this.inputOperator = inputOperator;
        this.updateLambdaBindable = updateLambdaBindable;
    }

    // PhysicalOperator interface

    @Override
    public Cursor cursor(StoreAdapter adapter, Bindings bindings) {
        Cursor inputCursor = inputOperator.cursor(adapter, bindings);
        return new Execution(inputCursor, updateLambdaBindable.bindTo(bindings));
    }

    @Override
    public List<PhysicalOperator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Object state

    private final PhysicalOperator inputOperator;
    private final Bindable<UpdateLambda> updateLambdaBindable;

    // Inner classes

    private class Execution extends ChainedCursor {

        private final UpdateLambda updateLambda;

        public Execution(Cursor input, UpdateLambda updateLambda) {
            super(input);
            this.updateLambda = updateLambda;
        }

        // Cursor interface

        @Override
        public boolean next() {
            if (input.next()) {
                Row row = this.input.currentRow();
                if (!updateLambda.rowIsApplicable(row)) {
                    return true;
                }
                Row currentRow = updateLambda.applyUpdate(row);
                input.updateCurrentRow(currentRow);
                return true;
            }
            return false;
        }
    }

    
}
