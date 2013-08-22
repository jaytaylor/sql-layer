/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.plan;

/** A context with some kind of loaded object. */
public abstract class UsingLoaderBase extends BasePlanWithInput
{
    private PlanNode loader;

    public UsingLoaderBase(PlanNode loader, PlanNode input) {
        super(input);
        this.loader = loader;
        loader.setOutput(this);
    }

    public PlanNode getLoader() {
        return loader;
    }
    public void setLoader(PlanNode loader) {
        this.loader = loader;
        loader.setOutput(this);
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        super.replaceInput(oldInput, newInput);
        if (loader == oldInput) {
            loader = newInput;
            loader.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            loader.accept(v);
            getInput().accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        loader = (PlanNode)loader.duplicate(map);
    }

}
