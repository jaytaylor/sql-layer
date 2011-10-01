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

import com.akiban.server.error.AkibanInternalException;

import java.util.*;

/** A join implementation using Map. */
public class MapJoin extends BasePlanNode implements PlanWithInput
{
    private PlanNode outer, inner;

    public MapJoin(PlanNode outer, PlanNode inner) {
        this.outer = outer;
        outer.setOutput(this);
        this.inner = inner;
        inner.setOutput(this);
    }

    public PlanNode getOuter() {
        return outer;
    }
    public void setOuter(PlanNode outer) {
        this.outer = outer;
        outer.setOutput(this);
    }
    public PlanNode getInner() {
        return inner;
    }
    public void setInner(PlanNode inner) {
        this.inner = inner;
        inner.setOutput(this);
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (outer == oldInput) {
            outer = newInput;
            outer.setOutput(this);
        }
        if (inner == oldInput) {
            inner = newInput;
            inner.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (outer.accept(v))
                inner.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        outer = (PlanNode)outer.duplicate(map);
        inner = (PlanNode)inner.duplicate(map);
    }

}
