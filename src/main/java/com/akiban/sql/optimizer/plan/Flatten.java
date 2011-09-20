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

/** Take heterogeneous rows and join into single rowset. */
// TODO: Decide whether this does product or only a single branch.
// Also whether non-group join conditions are moved out beforehand.
public class Flatten extends BasePlanWithInput
{
    private Joinable joins;

    public Flatten(PlanNode input, Joinable joins) {
        super(input);
        this.joins = joins;
    }

    public Joinable getJoins() {
        return joins;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                joins.accept(v);
            }
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        joins = (Joinable)joins.duplicate(map);
    }

}
