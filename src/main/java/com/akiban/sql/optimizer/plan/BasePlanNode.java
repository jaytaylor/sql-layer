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

public abstract class BasePlanNode extends BasePlanElement implements PlanNode
{
    private PlanWithInput output;

    protected BasePlanNode() {
    }

    @Override
    public PlanWithInput getOutput() {
        return output;
    }

    @Override
    public void setOutput(PlanWithInput output) {
        this.output = output;
    }

    @Override
    public String summaryString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16);
    }

    @Override
    public String toString() {
        return PlanToString.of(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy output or put it in the map: rely on copying
        // input to set back pointer.
    }

}
