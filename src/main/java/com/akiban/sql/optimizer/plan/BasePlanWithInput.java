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

package com.akiban.sql.optimizer.plan;

public abstract class BasePlanWithInput extends BasePlanNode
{
    private PlanNode input;

    protected BasePlanWithInput(PlanNode input) {
        this.input = input;
        input.setOutput(this);
    }

    public PlanNode getInput() {
        return input;
    }
    public void setInput(PlanNode input) {
        this.input = input;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            input.accept(v);
        }
        return v.visitLeave(this);
    }
    
}
