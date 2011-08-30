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
import com.akiban.util.ArgumentValidation;
import com.akiban.util.Strings;
import com.akiban.util.Tap;

public class Delete_Default implements UpdatePlannable {

    // constructor

    public Delete_Default(PhysicalOperator inputOperator, UpdateFunction deleteFunction) {
        ArgumentValidation.notNull("delete lambda", deleteFunction);
        
        this.inputOperator = inputOperator;
        this.deleteFunction = deleteFunction;
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), inputOperator, deleteFunction);
    }

    @Override
    public UpdateResult run(Bindings bindings, StoreAdapter adapter) {
        // TODO Auto-generated method stub
        return null;
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

    private final PhysicalOperator inputOperator;
    private final UpdateFunction deleteFunction;
    private static final Tap.InOutTap DELETE_TAP = Tap.createTimer("operator: delete");

}
