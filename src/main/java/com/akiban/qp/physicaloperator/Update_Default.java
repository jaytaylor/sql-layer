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

import com.akiban.qp.exec.CUDPlannable;
import com.akiban.qp.exec.CudResult;
import com.akiban.qp.row.Row;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.Strings;

import java.util.Collections;
import java.util.List;

public final class Update_Default implements CUDPlannable {

    // Object interface

    @Override
    public String toString() {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), inputOperator, updateFunction);
    }

    // constructor

    public Update_Default(PhysicalOperator inputOperator, UpdateFunction updateFunction) {
        ArgumentValidation.notNull("update lambda", updateFunction);
        if (!inputOperator.cursorAbilitiesInclude(CursorAbility.MODIFY)) {
            throw new IllegalArgumentException("input operator must be modifiable: " + inputOperator.getClass());
        }
        
        this.inputOperator = inputOperator;
        this.updateFunction = updateFunction;
    }

    // CudResult interface

    @Override
    public CudResult run(Bindings bindings, StoreAdapter adapter) {
        int touched = 0;
        long start = System.currentTimeMillis();
        Cursor inputCursor = inputOperator.cursor(adapter);
        Cursor execution =  new Execution(inputCursor, updateFunction);
        execution.open(bindings);
        try {
            while (execution.next()) {
                ++touched;
            }
        } finally {
            execution.close();
        }
        long end = System.currentTimeMillis();
        return new StandardCudResult(end - start, touched, touched);
    }

    // Plannable interface

    @Override
    public List<PhysicalOperator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    @Override
    public String describePlan(PhysicalOperator inputOperator) {
        return inputOperator + Strings.nl() + this;
    }

    // Object state

    private final PhysicalOperator inputOperator;
    private final UpdateFunction updateFunction;

    // Inner classes

    private class Execution extends ChainedCursor {

        private final UpdateFunction updateFunction;
        private Bindings bindings;

        public Execution(Cursor input, UpdateFunction updateFunction) {
            super(input);
            this.updateFunction = updateFunction;
            this.bindings = UndefBindings.only();
        }

        // Cursor interface


        @Override
        public void open(Bindings bindings) {
            super.open(bindings);
            this.bindings = bindings;
        }

        @Override
        public boolean next() {
            if (input.next()) {
                Row row = this.input.currentRow();
                if (!updateFunction.rowIsSelected(row)) {
                    return true;
                }
                Row currentRow = updateFunction.evaluate(row, bindings);
                input.updateCurrentRow(currentRow);
                return true;
            }
            return false;
        }
    }

    
}
